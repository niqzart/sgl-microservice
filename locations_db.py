from __future__ import annotations

from typing import Type, TypeVar

from sqlalchemy import Column, ForeignKey, select, delete
from sqlalchemy.orm import relationship
from sqlalchemy.sql.functions import count
from sqlalchemy.sql.sqltypes import Integer, String, Text, Float

from common import PydanticModel, Base

t = TypeVar("t", bound="LocalBase")


class LocalBase(Base):
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

    @classmethod
    def count(cls, session) -> int:
        return select(session.get_first(count(cls.id)))

    @classmethod
    def delete_all(cls, session):
        count: int = cls.count(session)
        session.execute(delete(cls))
        session.flush()
        return count


class Region(LocalBase):
    __tablename__ = "nq_regions"


class Municipality(LocalBase):
    __tablename__ = "nq_municipalities"

    reg_id = Column(Integer, ForeignKey("nq_regions.id"), nullable=False)
    reg = relationship("Region")

    TempModel = LocalBase.BaseModel.nest_model(LocalBase.BaseModel, "region", "reg")


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
