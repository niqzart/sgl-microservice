from __future__ import annotations

from flask_restx import Resource
from flask_restx.reqparse import RequestParser

from common import ResourceController, sessionmaker
from .locations_db import Place

controller = ResourceController("locations", sessionmaker=sessionmaker)


@controller.route("/")
class CitiesControlResource(Resource):
    parser = RequestParser()
    parser.add_argument("search", required=True)

    @controller.doc_abort(400, "Empty search")
    @controller.with_begin
    @controller.argument_parser(parser)
    @controller.marshal_list_with(Place.TempModel)
    def get(self, session, search: str) -> list[Place]:
        if len(search) == 0:
            controller.abort(400, "Empty search")
        return Place.get_all(session, search)
