from __future__ import annotations

from time import time

from flask_restx import Resource
from flask_restx.reqparse import RequestParser
from werkzeug.datastructures import FileStorage

from common import sessionmaker
from moderation import MUBNamespace, permission_index
from .locations_cli import manage_locations, upload_locations, delete_locations
from .locations_db import Place

controller = MUBNamespace("locations", path="/locations/", sessionmaker=sessionmaker)

CSV_HEADER = "id,region,municipality,settlement,type,population,children,latitude_dd,longitude_dd,oktmo"
STRATEGIES = (0, 1, 2, 3, 4, 5)


@controller.route("/")
class CitiesControlResource(Resource):
    parser = RequestParser()
    parser.add_argument("search", required=True)

    @controller.doc_abort(400, "Empty search")
    @permission_index.require_permission(controller, manage_locations, use_moderator=False)
    @controller.argument_parser(parser)
    @controller.marshal_list_with(Place.TempModel)
    def get(self, session, search: str) -> list[Place]:
        if len(search) == 0:
            controller.abort(400, "Empty search")
        times = {}
        for i in range(6):
            t = time()
            result = Place.get_all(session, search, strategy=i)
            times[i] = (time() - t) / 2
        for i in range(5, -1, -1):
            t = time()
            result = Place.get_all(session, search, strategy=i)
            times[i] += (time() - t) / 2
        print(times)
        return result

    parser = RequestParser()
    parser.add_argument("csv", location="files", type=FileStorage, required=True)

    @controller.doc_abort(400, "Invalid header")
    @controller.doc_abort("400 ", "Invalid line")
    @permission_index.require_permission(controller, manage_locations, use_moderator=False)
    @controller.argument_parser(parser)
    def post(self, session, csv: FileStorage):
        try:
            upload_locations(session, csv.stream)
        except ValueError as e:
            controller.abort(400, e.args[0])

    @permission_index.require_permission(controller, manage_locations, use_moderator=False)
    def delete(self, session):
        delete_locations(session)
