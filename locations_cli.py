from __future__ import annotations

from functools import wraps
from io import BytesIO
from json import load
from time import time
from typing import IO

from click import echo, argument, File, option
from flask import Blueprint, current_app

from common import sessionmaker
from moderation import permission_index
from .locations_db import Region, Municipality, SettlementType, Settlement, Place, County
from .locations_ini import locations_config

manage_locations = permission_index.add_permission("manage locations")
locations_cli_blueprint = Blueprint("locations", __name__)

CSV_HEADER = ("county", "region", "municipality", "settlement", "type", "population")
STRATEGIES = (0, 1, 2, 3, 4, 5)


def permission_cli_command(use_session: bool = True):
    def permission_cli_command_wrapper(function):
        @locations_cli_blueprint.cli.command(function.__name__.replace("_", "-"))
        @wraps(function)
        def permission_cli_command_inner(*args, **kwargs):
            if not permission_index.initialized:
                return echo("FATAL: Permission index has not been initialized")
            return function(*args, **kwargs)

        return sessionmaker.with_begin(permission_cli_command_inner) if use_session else permission_cli_command_inner

    return permission_cli_command_wrapper


def cache(dct, key, value_generator):
    if key not in dct:
        dct[key] = value_generator()
    return dct[key]


def mark_locations_updated(clear_cache: bool = True):
    locations_config.update_now(current_app, clear_cache)


def upload_locations(session, file: IO[bytes] | BytesIO, clear_cache: bool = True):
    header = file.readline().decode("utf-8").strip().split(",")
    if len(header) < 6 or header[:6] != CSV_HEADER:
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
        if len(params) < 6:
            raise ValueError("Invalid line: " + line)
        county_name, reg_name, mun_name, set_name, set_type, population = params[:6]
        if mun_name == "null":
            mun_name = reg_name

        cty = cache(counties, county_name, lambda: County.create(session, county_name).id)
        reg = cache(regions, reg_name, lambda: list(Region.create_with_place(session, reg_name, cty)))[0]
        mun = cache(municipalities, mun_name, lambda: list(
            Municipality.create_with_place(session, mun_name, reg_id=reg.id)))[0]
        type_id = cache(types, set_type, lambda: SettlementType.find_or_create(session, set_type).id)

        Settlement.create_with_place(session, mun.id, type_id, set_name, int(population))
        regions[reg_name][2] += population
        municipalities[mun_name][2] += population

    for _, place, population in list(regions.values()) + list(municipalities.values()):
        place.population = population

    print(Place.count(session))
    locations_config.update_now(current_app, clear_cache)


def delete_locations(session, clear_cache: bool = True):
    Place.delete_all(session)
    Settlement.delete_all(session)
    SettlementType.delete_all(session)
    Municipality.delete_all(session)
    Region.delete_all(session)
    locations_config.update_now(current_app, clear_cache)


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


@permission_cli_command(False)
@option("-s", "--save-cache", is_flag=True)
def mark_updated(save_cache: bool):
    mark_locations_updated(not save_cache)


@permission_cli_command()
@argument("csv", type=File("rb"))
@option("-s", "--save-cache", is_flag=True)
def upload(session, csv: IO[bytes], save_cache: bool):
    try:
        upload_locations(session, csv, not save_cache)
    except ValueError as e:
        print(e.args[0])


@permission_cli_command()
@option("-s", "--save-cache", is_flag=True)
def delete(session, save_cache: bool):
    delete_locations(session, not save_cache)


@permission_cli_command()
@argument("data", type=File(encoding="utf-8"))
def test(session, data):
    test_search(session, load(data))
