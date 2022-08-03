from __future__ import annotations

from typing import Type, TypeVar, Iterable

from sqlalchemy import Column, ForeignKey, select, delete, or_, and_, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql.functions import count
from sqlalchemy.sql.sqltypes import Integer, String, Text, Float

from common import PydanticModel, Base

t = TypeVar("t", bound="LocalBase")


class DeleteAllAble(Base):
    __abstract__ = True

    @classmethod
    def delete_all(cls, session):
        print(cls.count(session))
        session.execute(delete(cls))
        session.flush()

    @classmethod
    def count(cls, session) -> int:
        return select(session.get_first(count(cls.id)))


class LocalBase(DeleteAllAble):
    __abstract__ = True

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    aliases = Column(Text, nullable=True)

    BaseModel = PydanticModel.column_model(id=id, name=name, aliases=aliases)

    @classmethod
    def find_or_create(cls: Type[t], session, name: str, **kwargs) -> t:
        entry = session.get_first(select(cls).filter_by(name=name))
        if entry is None:
            entry = cls.create(session, name=name, **kwargs)
        return entry


class Region(LocalBase):
    __tablename__ = "nq_regions"

    @classmethod
    def create_with_place(cls, session, name: str) -> tuple[Region, Place, int]:
        result = super().create(session, name=name)
        return result, Place.create(session, name, result.id), 0


class Municipality(LocalBase):
    __tablename__ = "nq_municipalities"

    reg_id = Column(Integer, ForeignKey("nq_regions.id"), nullable=False)
    reg = relationship("Region")

    TempModel = LocalBase.BaseModel.nest_model(LocalBase.BaseModel, "region", "reg")

    @classmethod
    def create_with_place(cls, session, name: str, reg_id: int) -> tuple[Municipality, Place, int]:
        result = super().create(session, name=name, reg_id=reg_id)
        return result, Place.create(session, name, reg_id, result.id), 0


class SettlementType(LocalBase):
    __tablename__ = "nq_settlement_types"


class Settlement(LocalBase):
    __tablename__ = "nq_settlements"

    mun_id = Column(Integer, ForeignKey("nq_municipalities.id"), nullable=False)
    mun = relationship("Municipality")
    type_id = Column(Integer, ForeignKey("nq_settlement_types.id"), nullable=False)
    type = relationship("SettlementType")

    population = Column(Integer, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    oktmo = Column(String(11), nullable=False)

    FullModel = LocalBase.BaseModel.column_model(population, latitude, longitude, oktmo)

    @classmethod
    def create_with_place(cls, session, mun_id: int, type_id: int, name: str, oktmo: str,
                          population: int, latitude: float, longitude: float) -> tuple[Municipality, Place]:
        result = super().create(session, mun_id=mun_id, type_id=type_id, name=name, oktmo=oktmo,
                                population=population, latitude=latitude, longitude=longitude)
        return result, Place.create(session, name, result.mun.reg_id, mun_id, type_id, result.id, population)


def ilike_with_none(column: Column, search: str):
    return or_(column.ilike(search), None)


class Place(DeleteAllAble):
    __tablename__ = "nq_place"

    id = Column(Integer, primary_key=True)
    reg_id = Column(Integer, ForeignKey("nq_regions.id"), nullable=False)
    reg = relationship("Region", foreign_keys=[reg_id])
    mun_id = Column(Integer, ForeignKey("nq_municipalities.id"), nullable=True)
    mun = relationship("Municipality", foreign_keys=[mun_id])
    type_id = Column(Integer, ForeignKey("nq_settlement_types.id"), nullable=True)
    type = relationship("SettlementType", foreign_keys=[type_id])
    set_id = Column(Integer, ForeignKey("nq_settlements.id"), nullable=True)
    settlement = relationship("Settlement", foreign_keys=[set_id])

    population = Column(Integer, nullable=False)
    name = Column(Text, nullable=False)

    ALLOWED_SYMBOLS: set[str] = set(" \"()+-./0123456789<>ENU_clnux«»ЁАБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЫЭЮЯ"
                                    "абвгдежзийклмнопрстуфхцчшщъыьэюяё—№")
    JOINS = [(Region, reg_id), (Municipality, mun_id), (Settlement, set_id)]
    STRATEGY: int = 0
    TOTAL: int = None
    TRY_LENGTHS: Iterable[int] = (4, 10)

    TempModel = PydanticModel \
        .nest_model(LocalBase.BaseModel, "region", "reg") \
        .nest_model(LocalBase.BaseModel, "municipality", "mun") \
        .nest_model(Settlement.FullModel, "settlement") \
        .nest_model(LocalBase.BaseModel, "type")

    @classmethod
    def create(cls, session, name: str, reg_id: int, mun_id: int = None,
               type_id: int = None, set_id: int = None, population: int = 0) -> Place:
        return super().create(session, name=name, reg_id=reg_id, mun_id=mun_id,
                              type_id=type_id, set_id=set_id, population=population)

    @classmethod
    def get_all(cls, session, search: str, total: int = None, strategy: int = None) -> list[Place]:
        if len(search) > 60 or any(sym not in cls.ALLOWED_SYMBOLS for sym in search):
            return []

        if strategy is None:
            strategy = cls.STRATEGY
        if total is None:
            total = cls.TOTAL or (100 // (len(search) * 2) if len(search) < 6 else 5)
        print(total)
        search_pattern = search + "%"

        def rank(place: Place):
            if place.mun_id is None:
                return 20
            if place.set_id is None:
                return 50 if search in place.reg.name else 10
            result = 0
            if search in place.reg.name:
                result += 50
            if search in place.mun.name:
                result += 20
            return result

        def full_rank(place: Place):
            return rank(place), place.population

        if strategy % 2 == 0:
            for length in cls.TRY_LENGTHS:
                if len(search) > length:
                    results = cls.get_all(session, search[:length], total + 1)
                    if results != total + 1:
                        results = [r for r in results if search.lower() in r.name.lower()]
                        results.sort(key=full_rank, reverse=True)
                        return results

        if strategy // 4 == 0:
            if strategy == 1 and len(search) > 4:
                results = session.get_all(select(cls).filter(cls.name.ilike(search[:4] + "%"))
                                          .order_by(cls.population).limit(total + 1))
                if len(results) != total + 1:
                    results = [r for r in results if search.lower() in r.name.lower()]
                    results.sort(key=full_rank, reverse=True)
                    return results

            results: list[Place] = []
            result_ids = set()
            stmt = select(Settlement.id).order_by(Settlement.population.desc())
            p_stmt = select(cls).order_by(cls.population.desc())

            def place_all(places: list[Place]):
                places = [p for p in places if p.id not in result_ids]
                results.extend(places)
                result_ids.update(set(p.id for p in places))
                return len(results) >= total

            def place_all_set(subquery):
                subquery = subquery.filter(Settlement.name.ilike(search_pattern)).limit(total - len(result_ids))
                stmt = p_stmt.filter(cls.id.not_in(result_ids)).filter(cls.set_id.in_(subquery))
                return place_all(session.get_all(stmt))

            def place_all_other(query):
                query = query.filter(cls.id.not_in(result_ids)).limit(total - len(result_ids))
                return place_all(session.get_all(query))

            regions = session.get_all(select(Region.id).filter(Region.name.ilike(search_pattern)))
            municipalities = session.get_all(select(Municipality.id).filter(Municipality.name.ilike(search_pattern)))
            mun_stmt = select(Municipality.id).filter(Municipality.id.in_(municipalities))

            if len(regions):
                mun_regs = session.get_all(mun_stmt.filter(Municipality.reg_id.in_(regions)))
                if len(mun_regs) and place_all_set(stmt.filter(Settlement.mun_id.in_(mun_regs))):
                    return results

                if place_all_other(p_stmt.filter(cls.reg_id.in_(regions), cls.set_id.is_not(None),
                                                 cls.name.ilike(search_pattern))):
                    return results

            if len(regions) and place_all_other(p_stmt.filter(cls.mun_id.is_(None), cls.reg_id.in_(regions))):
                return results

            if len(municipalities) and place_all_other(
                    p_stmt.filter(cls.set_id.is_(None), cls.mun_id.in_(municipalities))):
                return results

            place_all_set(stmt)

            return results

        elif strategy // 4 == 1:
            stmt = select(cls).order_by(cls.population.desc())
            result_ids = set()
            results = session.get_all(
                stmt.limit(total)
                .filter(cls.set_id.is_not(None), cls.name.ilike(search_pattern))
                .join(Region, and_(cls.reg_id == Region.id, Region.name.ilike(search_pattern)))
                .join(Municipality, and_(cls.mun_id == Municipality.id, Municipality.name.ilike(search_pattern)))
            )
            result_ids.update(set(r.id for r in results))

            results += session.get_all(
                stmt.limit(total - len(results))
                .filter(cls.id.notin_(result_ids), cls.set_id.is_not(None), cls.name.ilike(search_pattern))
                .join(Region, and_(cls.reg_id == Region.id, Region.name.ilike(search_pattern)))
            )
            result_ids.update(set(r.id for r in results))

            results += session.get_all(
                stmt.limit(total - len(results))
                .filter(cls.id.notin_(result_ids), cls.mun_id.is_(None))
                .join(Region, and_(cls.reg_id == Region.id, Region.name.ilike(search_pattern)))
            )
            result_ids.update(set(r.id for r in results))

            results += session.get_all(
                stmt.limit(total - len(results))
                .filter(cls.id.notin_(result_ids), cls.set_id.is_(None), cls.mun_id.is_not(None))
                .join(Municipality, and_(cls.mun_id == Municipality.id, Municipality.name.ilike(search_pattern)))
            )
            result_ids.update(set(r.id for r in results))

            results += session.get_all(
                stmt.limit(total - len(results))
                .filter(cls.id.notin_(result_ids), cls.set_id.is_not(None), cls.name.ilike(search_pattern))
            )
            result_ids.update(set(r.id for r in results))

            return results

        stmt = select(cls)
        for part, column in cls.JOINS:
            stmt = stmt.outerjoin(part, column == part.id)

        stmt = stmt.filter(or_(
            and_(Region.name.ilike(search_pattern), cls.set_id.is_(None), cls.mun_id.is_(None)),
            and_(Municipality.name.ilike(search_pattern), cls.set_id.is_(None), cls.mun_id.is_not(None)),
            and_(Settlement.name.ilike(search_pattern), cls.set_id.is_not(None), cls.mun_id.is_not(None)),
        ))

        stmt = stmt.order_by(
            and_(ilike_with_none(Region.name, search_pattern), ilike_with_none(Municipality.name, search_pattern),
                 ilike_with_none(Settlement.name, search_pattern)),
            and_(ilike_with_none(Region.name, search_pattern), ilike_with_none(Settlement.name, search_pattern)),
            and_(ilike_with_none(Region.name, search_pattern), ilike_with_none(Settlement.name, search_pattern)),
            and_(ilike_with_none(Region.name, search_pattern), ilike_with_none(Municipality.name, search_pattern)),
            ilike_with_none(Region.name, search_pattern),
            ilike_with_none(Municipality.name, search_pattern),
            ilike_with_none(Settlement.name, search_pattern),
            cls.population.desc()
        )

        return session.get_all(stmt.limit(total))


Index("idx_nq_region_name", Region.name)
Index("idx_nq_municipality_name", Municipality.name)
Index("idx_nq_settlement_name", Settlement.name)
Index("idx_nq_settlement_population", Settlement.population.desc())

Index("idx_nq_place_name", Place.name)
Index("idx_nq_place_reg_id", Place.reg_id)
Index("idx_nq_place_mun_id", Place.mun_id)
Index("idx_nq_place_set_id", Place.set_id)
Index("idx_nq_place_population", Place.population.desc())
Index("idx_nq_place_name_population", Place.name, Place.population.desc())
