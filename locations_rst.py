from __future__ import annotations

from functools import wraps

from flask import request, current_app, jsonify
from flask_restx import Resource
from flask_restx.reqparse import RequestParser

from common import ResourceController, sessionmaker
from .locations_db import Place, County
from .locations_ini import locations_config

controller = ResourceController("locations", sessionmaker=sessionmaker)


def with_caching():
    def with_caching_wrapper(function):
        @wraps(function)
        def with_caching_inner(*args, **kwargs):
            if current_app.config.get("NQ_DISABLE_CACHING", False):
                return function(*args, **kwargs)
            if locations_config.compare_expiry(request.if_modified_since):
                return "", 304
            response = jsonify(function(*args, **kwargs))
            response.last_modified = locations_config.last_modified
            response.headers.add_header("X-Accel-Expires", "@1")
            return response

        return with_caching_inner

    return with_caching_wrapper


@controller.route("/search/")
class LocationsSearcher(Resource):
    parser = RequestParser()
    parser.add_argument("search", required=True)

    @controller.doc_abort(400, "Empty search")
    @with_caching()
    @controller.with_begin
    @controller.argument_parser(parser)
    @controller.marshal_list_with(Place.CompressedModel)
    def get(self, session, search: str):
        if len(search) == 0:
            controller.abort(400, "Empty search")
        return Place.get_all(session, search)


@County.BaseModel.include_context("session")
class CountyIndexModel(County.BaseModel):
    regions: list[Place.RegionModel]

    @classmethod
    def callback_convert(cls, callback, orm_object: County, session=None, **_):
        callback(regions=[Place.RegionModel.convert(reg)
                          for reg in Place.get_regions_by_county(session, orm_object.id)])


@controller.route("/counties/")
class CountiesTreeer(Resource):
    @with_caching()
    @controller.with_begin
    @controller.marshal_list_with(CountyIndexModel)
    def get(self, session) -> list[County]:
        return County.get_all(session)


@controller.route("/regions/<int:place_id>/settlements/")
class RegionsTreeer(Resource):
    @with_caching()
    @controller.with_begin
    @controller.database_searcher(Place, use_session=True)
    @controller.marshal_list_with(Place.SettlementModel)
    def get(self, session, place: Place) -> list[Place]:
        """Top-20 most populated settlements of this region"""
        return Place.get_most_populous(session, place.reg_id)
