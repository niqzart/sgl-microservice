from __future__ import annotations

from functools import wraps
from io import BytesIO
from json import load
from time import time
from typing import IO

from click import echo, argument, File
from flask import Blueprint, current_app

from common import sessionmaker
from moderation import permission_index
from .locations_db import Region, Municipality, SettlementType, Settlement, Place, County
from .locations_ini import locations_config

manage_locations = permission_index.add_permission("manage locations")
locations_cli_blueprint = Blueprint("locations", __name__)

CSV_HEADER = "county,region,municipality,settlement,type,population,children,latitude_dd,longitude_dd,oktmo"
STRATEGIES = (0, 1, 2, 3, 4, 5)


def permission_cli_command():
    def permission_cli_command_wrapper(function):
        @locations_cli_blueprint.cli.command(function.__name__.replace("_", "-"))
        @wraps(function)
        @sessionmaker.with_begin
        def permission_cli_command_inner(*args, **kwargs):
            if not permission_index.initialized:
                return echo("FATAL: Permission index has not been initialized")
            return function(*args, **kwargs)

        return permission_cli_command_inner

    return permission_cli_command_wrapper


def cache(dct, key, value_generator):
    if key not in dct:
        dct[key] = value_generator()
    return dct[key]


def upload_locations(session, file: IO[bytes] | BytesIO):
    if not file.readline().decode("utf-8").strip() == CSV_HEADER:
        raise ValueError("Invalid header")
    lines = file.readlines()
    notify = len(lines) // 20
    t = time()
    c = time()

    counties: dict[str, int] = {}
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
        params = [term.strip() for term in line.strip().split(",")]
        if len(params) != 10:
            raise ValueError("Invalid line: " + line)
        county_name, reg_name, mun_name, set_name, set_type, population, children, latitude, longitude, oktmo = params
        if mun_name == "null":
            mun_name = reg_name

        cty = cache(counties, county_name, lambda: County.create(session, county_name).id)
        reg = cache(regions, reg_name, lambda: list(Region.create_with_place(session, reg_name, cty)))[0]
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
    locations_config.update_now(current_app)


def delete_locations(session):
    Place.delete_all(session)
    Settlement.delete_all(session)
    SettlementType.delete_all(session)
    Municipality.delete_all(session)
    Region.delete_all(session)
    locations_config.update_now(current_app)


def time_one(session, search: str, strategy: int) -> tuple[float, set[int]]:
    count = 4
    speed = 0
    results = set(place.id for place in Place.get_all(session, search, strategy=strategy))
    for _ in range(count):
        timer = time()
        Place.get_all(session, search, strategy=strategy)
        speed += (time() - timer) / count
    return speed, results


def test_search(session, test_searches: dict[str, list[str]]):
    for group_name, group in test_searches.items():
        check_sum: dict[str, set[int]] = {}
        print(f"\nSummary for {group_name}:")
        for strategy in STRATEGIES:
            speeds: list[float] = []
            for test_search in group:
                speed, results = time_one(session, test_search, strategy)
                speeds.append(speed)

                if test_search not in check_sum:
                    check_sum[test_search] = results
                if len(diff := results.symmetric_difference(check_sum[test_search])):
                    print(f"[strategy {strategy}] Some ids don't match for {test_search}: ", diff)
            print(f"[strategy {strategy}]", *speeds)


@permission_cli_command()
@argument("csv", type=File("rb"))
def upload(session, csv: IO[bytes]):
    try:
        upload_locations(session, csv)
    except ValueError as e:
        print(e.args[0])


@permission_cli_command()
def delete(session):
    delete_locations(session)


@permission_cli_command()
@argument("data", type=File(encoding="utf-8"))
def test(session, data):
    test_search(session, load(data))
