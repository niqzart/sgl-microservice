from __future__ import annotations

from functools import wraps

from flask import request, current_app, jsonify, Response
from flask_caching import Cache
from flask_restx import Resource
from flask_restx.reqparse import RequestParser

from common import ResourceController, sessionmaker
from .locations_db import Place, County, Region
from .locations_ini import locations_config


def with_revalidate():
    def with_revalidate_wrapper(function):
        @wraps(function)
        def with_revalidate_inner(*args, **kwargs):
            if not current_app.config.get("NQ_DISABLE_REVALIDATION", False) \
                    and locations_config.compare_expiry(request.if_modified_since):
                return Response(status=304)

            response = function(*args, **kwargs)
            response.last_modified = locations_config.last_modified
            response.headers.add_header("X-Accel-Expires", "@1")
            return response

        return with_revalidate_inner

    return with_revalidate_wrapper


def parse_search(controller):
    parser = RequestParser()
    parser.add_argument("search", required=True)

    def parse_search_wrapper(function):
        @controller.doc_abort(400, "Empty search")
        @wraps(function)
        @controller.argument_parser(parser)
        def parse_search_inner(*args, search, **kwargs):
            if len(search) == 0:
                response = jsonify("Empty search")
                response.status = 400
                return response
            if Place.is_search_invalid(search):
                return jsonify([])
            return function(*args, search=search, **kwargs)

        return parse_search_inner

    return parse_search_wrapper


def with_caching(cache: Cache, key_prefix: str, cache_key: str = None):
    def with_caching_wrapper(function):
        @wraps(function)
        def with_caching_inner(*args, **kwargs):
            key = key_prefix
            if cache_key is not None:
                key += str(kwargs[cache_key])
            if cache.has(key):
                return cache.get(key)

            response = jsonify(function(*args, **kwargs))
            cache.set(key, response)
            return response

        return with_caching_inner

    return with_caching_wrapper


def setup(controller: ResourceController = None, search_cache=None, important_cache=None) -> ResourceController:
    if controller is None:
        controller = ResourceController("locations", sessionmaker=sessionmaker)

    class LocationsSearcher(Resource):
        @with_revalidate()
        @parse_search(controller)
        @with_caching(search_cache, "search-", "search")
        @controller.with_begin
        @controller.marshal_list_with(Place.CompressedModel)
        def get(self, session, search: str):
            return Place.get_all(session, search)

    @County.BaseModel.include_context("session")
    class CountyIndexModel(County.BaseModel):
        regions: list[Place.RegionModel]

        @classmethod
        def callback_convert(cls, callback, orm_object: County, session=None, **_):
            callback(regions=[Place.RegionModel.convert(reg)
                              for reg in Place.get_regions_by_county(session, orm_object.id)])

    class CountiesTreeer(Resource):
        @with_revalidate()
        @with_caching(important_cache, "counties")
        @controller.with_begin
        @controller.marshal_list_with(CountyIndexModel)
        def get(self, session) -> list[County]:
            return County.get_all(session)

    class RegionsTreeer(Resource):
        @with_revalidate()
        @controller.with_begin
        @controller.database_searcher(Region, use_session=True, check_only=True)
        @with_caching(important_cache, "region-", "region_id")
        @controller.marshal_list_with(Place.SettlementModel)
        def get(self, session, region_id: int) -> list[Place]:
            """Top-20 most populated settlements of this region"""
            return Place.get_most_populous(session, region_id)

    controller.route("/search/")(LocationsSearcher)
    controller.route("/counties/")(CountiesTreeer)
    controller.route("/regions/<int:region_id>/settlements/")(RegionsTreeer)

    return controller
