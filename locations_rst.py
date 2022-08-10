from __future__ import annotations

from flask_restx import Resource
from flask_restx.reqparse import RequestParser

from common import ResourceController, sessionmaker
from .locations_db import Place, County, Region

controller = ResourceController("locations", sessionmaker=sessionmaker)


@controller.route("/search/")
class LocationsSearcher(Resource):
    parser = RequestParser()
    parser.add_argument("search", required=True)

    @controller.doc_abort(400, "Empty search")
    @controller.with_begin
    @controller.argument_parser(parser)
    @controller.marshal_list_with(Place.FullModel)
    def get(self, session, search: str) -> list[Place]:
        if len(search) == 0:
            controller.abort(400, "Empty search")
        return Place.get_all(session, search)


class CountyIndexModel(County.BaseModel):
    regions: list[Region.BaseModel]

    @classmethod
    def callback_convert(cls, callback, orm_object: County, **_):
        callback(regions=[Region.BaseModel.convert(reg) for reg in orm_object.regions])


@controller.route("/counties/")
class LocationsTreeer(Resource):
    @controller.with_begin
    @controller.marshal_list_with(CountyIndexModel)
    def get(self, session) -> list[County]:
        return County.get_all(session)


@controller.route("/regions/<int:region_id>/settlements/")
class LocationsTreeer(Resource):
    @controller.with_begin
    @controller.database_searcher(Region, use_session=True, check_only=True)
    @controller.marshal_list_with(Place.SetMunModel)
    def get(self, session, region_id: int) -> list[Place]:
        """Top-20 most populated settlements of this region"""
        return Place.get_most_populous(session, region_id)
