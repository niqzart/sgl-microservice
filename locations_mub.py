from __future__ import annotations

from flask_restx import Resource
from flask_restx.reqparse import RequestParser
from werkzeug.datastructures import FileStorage

from common import sessionmaker
from moderation import MUBNamespace, permission_index
from .locations_db import Region, Municipality, SettlementType, Settlement, Place

manage_cities = permission_index.add_permission("manage locations")
controller = MUBNamespace("locations", path="/locations/", sessionmaker=sessionmaker)


@controller.route("/")
class CitiesControlResource(Resource):
    parser = RequestParser()
    parser.add_argument("search", required=True)

    @permission_index.require_permission(controller, manage_cities, use_moderator=False)
    @controller.argument_parser(parser)
    @controller.marshal_list_with(Place.TempModel)
    def get(self, session, search: str) -> list[Place]:
        if len(search) == 0:
            controller.abort(400, "Empty search")
        return Place.find_by_name(session, search)

    parser = RequestParser()
    parser.add_argument("csv", location="files", type=FileStorage, required=True)

    @permission_index.require_permission(controller, manage_cities, use_moderator=False)
    @controller.argument_parser(parser)
    def post(self, session, csv: FileStorage):
        lines = csv.stream.readlines()
        for line in lines[1:]:
            line = line.decode("utf-8")
            params = [term.strip() for term in line.strip().split(",")[1:]]
            if len(params) != 9:
                controller.abort(400, "Invalid line: " + line)
            reg_name, mun_name, set_name, set_type, population, children, latitude, longitude, oktmo = params
            if mun_name == "null":
                mun_name = reg_name

            reg = Region.find_or_create(session, reg_name)
            mun = Municipality.find_or_create(session, mun_name, reg_id=reg.id)
            settlement_type = SettlementType.find_or_create(session, set_type)

            population = int(population) + int(children)
            Settlement.create(session, mun_id=mun.id, type_id=settlement_type.id, name=set_name,
                              oktmo=oktmo, population=population, latitude=float(latitude), longitude=float(longitude))

    @permission_index.require_permission(controller, manage_cities, use_moderator=False)
    def delete(self, session):
        Settlement.delete_all(session)
        SettlementType.delete_all(session)
        Municipality.delete_all(session)
        Region.delete_all(session)
