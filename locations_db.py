from __future__ import annotations

from typing import Type, TypeVar

from sqlalchemy import Column, ForeignKey, select, delete
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
        entry = cls.find_by_name(session, name, **kwargs)
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
    set = relationship("Settlement", foreign_keys=[set_id])

    population = Column(Integer, nullable=False)
    name = Column(Text, nullable=False)

    TempModel = PydanticModel \
        .nest_model(LocalBase.BaseModel, "region", "reg") \
        .nest_model(LocalBase.BaseModel, "municipality", "mun") \
        .nest_model(Settlement.FullModel, "settlement", "set") \
        .nest_model(LocalBase.BaseModel, "type")

    @classmethod
    def create(cls, session, name: str, reg_id: int, mun_id: int = None,
               type_id: int = None, set_id: int = None, population: int = 0) -> Place:
        return super().create(session, name=name, reg_id=reg_id, mun_id=mun_id,
                              type_id=type_id, set_id=set_id, population=population)

    @classmethod
    def find_by_name(cls, session, name: str) -> Place:
        return session.get_all(select(cls).filter(cls.name.ilike(name + "%")))
