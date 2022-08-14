from __future__ import annotations

from time import time

from flask_restx import Resource
from flask_restx.reqparse import RequestParser
from werkzeug.datastructures import FileStorage

from common import sessionmaker
from moderation import MUBController
from .locations_cli import manage_locations, upload_locations, delete_locations, mark_locations_updated
from .locations_db import Place


def setup(controller: MUBController = None) -> MUBController:
    if controller is None:
        controller = MUBController("locations", path="/locations/", sessionmaker=sessionmaker)

    class CitiesControlResource(Resource):
        parser = RequestParser()
        parser.add_argument("search", required=True)
        parser.add_argument("strategy", type=int, required=False)

        @controller.doc_abort(400, "Empty search")
        @controller.require_permission(manage_locations, use_moderator=False)
        @controller.argument_parser(parser)
        @controller.marshal_list_with(Place.FullModel)
        def get(self, session, search: str, strategy: int) -> list[Place]:
            if len(search) == 0:
                controller.abort(400, "Empty search")
            t = time()
            result = Place.get_all(session, search, strategy=strategy)
            print(time() - t)

            a = [(r.id, r.name) for r in result]
            b = [(r.id, r.name) for r in Place.get_all(session, search, strategy=0 if strategy == 4 else 4)]
            for i in range(len(a)):
                if a[i] != b[i]:
                    print(i, a[i], b[i])
            print()

            return result

        parser = RequestParser()
        parser.add_argument("csv", location="files", type=FileStorage, required=True)

        @controller.doc_abort(400, "Invalid header")
        @controller.doc_abort("400 ", "Invalid line")
        @controller.require_permission(manage_locations, use_moderator=False)
        @controller.argument_parser(parser)
        def post(self, session, csv: FileStorage):
            try:
                upload_locations(session, csv.stream)
            except ValueError as e:
                controller.abort(400, e.args[0])

        @controller.require_permission(manage_locations, use_moderator=False)
        def delete(self, session):
            delete_locations(session)

    class UpdatedMarkResource(Resource):
        @controller.require_permission(manage_locations, use_session=False, use_moderator=False)
        def post(self):
            mark_locations_updated()

    controller.route("/")(CitiesControlResource)
    controller.route("/mark-updated/")(UpdatedMarkResource)

    return controller
