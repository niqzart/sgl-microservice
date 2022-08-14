from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from json import load, dump

from flask import Flask


def prepare_datetime(dt: datetime) -> int:
    return int(dt.timestamp())


@dataclass()
class LocationsConfig:
    last_modified: datetime = None
    caches: list = None

    def save(self, app: Flask):
        with open(app.config.get("NQ_LOCATIONS_CONFIG_PATH", "locations.json"), "w", encoding="utf-8") as f:
            dump({"updated": locations_config.last_modified.isoformat()}, f, ensure_ascii=False)

    def update_now(self, app: Flask, clear_cache: bool = True):
        self.last_modified = datetime.now(timezone.utc)
        self.save(app)
        if clear_cache:
            for cache in self.caches or []:
                cache.clear()

    def load(self, app: Flask, clear_cache: bool = True):
        try:
            with open(app.config.get("NQ_LOCATIONS_CONFIG_PATH", "locations.json"), "r", encoding="utf-8") as f:
                self.last_modified = datetime.fromisoformat(load(f)["updated"])
        except (FileNotFoundError, ValueError):
            self.update_now(app, clear_cache)

    def compare_expiry(self, other_date: datetime | None):
        return other_date is not None and prepare_datetime(self.last_modified) <= prepare_datetime(other_date)


locations_config = LocationsConfig()


def init_locations(app, *caches):
    locations_config.load(app)
    locations_config.caches = caches
