from __future__ import annotations

from time import time
from flask_restx import Resource
from flask_restx.reqparse import RequestParser
from werkzeug.datastructures import FileStorage

from common import sessionmaker
from moderation import MUBNamespace, permission_index
from .locations_db import Region, Municipality, SettlementType, Settlement, Place

manage_cities = permission_index.add_permission("manage locations")
controller = MUBNamespace("locations", path="/locations/", sessionmaker=sessionmaker)

CSV_HEADER = "id,region,municipality,settlement,type,population,children,latitude_dd,longitude_dd,oktmo"
STRATEGIES = (0, 1)


def cache(dct, key, value_generator):
    if key not in dct:
        dct[key] = value_generator()
    return dct[key]


@controller.route("/")
class CitiesControlResource(Resource):
    parser = RequestParser()
    parser.add_argument("search", required=True)

    @controller.doc_abort(400, "Empty search")
    @permission_index.require_permission(controller, manage_cities, use_moderator=False)
    @controller.argument_parser(parser)
    @controller.marshal_list_with(Place.TempModel)
    def get(self, session, search: str) -> list[Place]:
        if len(search) == 0:
            controller.abort(400, "Empty search")
        times = []
        for i in STRATEGIES:
            t = time()
            result = Place.get_all(session, search, strategy=i)
            times.append(time() - t)
        print(*times)
        return result

    parser = RequestParser()
    parser.add_argument("csv", location="files", type=FileStorage, required=True)

    @controller.doc_abort(400, "Invalid header")
    @controller.doc_abort("400 ", "Invalid line")
    @permission_index.require_permission(controller, manage_cities, use_moderator=False)
    @controller.argument_parser(parser)
    def post(self, session, csv: FileStorage):
        if not csv.stream.readline().decode("utf-8").strip() == CSV_HEADER:
            controller.abort(400, "Invalid header")
        lines = csv.stream.readlines()
        notify = len(lines) // 20
        t = time()
        c = time()

        regions: dict[str, list[Region, Place, int]] = {}
        municipalities: dict[str, list[Municipality, Place, int]] = {}
        types: dict[str, int] = {}

        for i, line in enumerate(lines):
            if i != 0 and i % notify == 0:
                percent = i // notify
                elapsed_t = time() - t
                elapsed_c = time() - c
                print(f"{percent * 5:3}% | Time elapsed: {elapsed_t}s | "
                      f"Time for step: {elapsed_c}s | ETA: {elapsed_c * (20 - percent)}s")
                c = time()

            line = line.decode("utf-8")
            params = [term.strip() for term in line.strip().split(",")[1:]]
            if len(params) != 9:
                controller.abort(400, "Invalid line: " + line)
            reg_name, mun_name, set_name, set_type, population, children, latitude, longitude, oktmo = params
            if mun_name == "null":
                mun_name = reg_name

            reg = cache(regions, reg_name, lambda: list(Region.create_with_place(session, reg_name)))[0]
            mun = cache(municipalities, mun_name, lambda: list(
                Municipality.create_with_place(session, mun_name, reg_id=reg.id)))[0]
            type_id = cache(types, set_type, lambda: SettlementType.find_or_create(session, set_type).id)

            population = int(population) + int(children)
            Settlement.create_with_place(session, mun.id, type_id, set_name, oktmo, population,
                                         float(latitude), float(longitude))
            regions[reg_name][2] += population
            municipalities[mun_name][2] += population

        for _, place, population in list(regions.values()) + list(municipalities.values()):
            place.population = population

        print(Place.count(session))

    @permission_index.require_permission(controller, manage_cities, use_moderator=False)
    def delete(self, session):
        Place.delete_all(session)
        Settlement.delete_all(session)
        SettlementType.delete_all(session)
        Municipality.delete_all(session)
        Region.delete_all(session)
