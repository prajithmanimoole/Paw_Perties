"""SQLAlchemy ORM models and Pydantic request/response schemas for Pawperty."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, field_validator
from sqlalchemy import Boolean, Column, DateTime, Enum, Integer, String
from sqlalchemy.sql import func

from database import Base


# ---------------------------------------------------------------------------
# SQLAlchemy ORM
# ---------------------------------------------------------------------------

class User(Base):
    """Application user stored in PostgreSQL."""

    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    hashed_password = Column(String(256), nullable=False)
    role = Column(Enum("admin", "officer", name="user_role"), nullable=False, default="officer")
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class Citizen(Base):
    """End-user citizen stored in PostgreSQL (separate from admin/officer users)."""

    __tablename__ = "citizens"

    id = Column(Integer, primary_key=True, index=True)
    customer_key = Column(String(16), unique=True, nullable=False, index=True)
    name = Column(String(128), nullable=False)
    aadhar_no = Column(String(12), unique=True, nullable=False)
    pan_no = Column(String(10), unique=True, nullable=False)
    hashed_password = Column(String(256), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


# ---------------------------------------------------------------------------
# Pydantic — Auth
# ---------------------------------------------------------------------------

class LoginForm(BaseModel):
    username: str
    password: str


class CitizenLoginForm(BaseModel):
    customer_key: str
    password: str


class CitizenRegisterForm(BaseModel):
    name: str
    aadhar_no: str
    pan_no: str
    password: str
    customer_key: Optional[str] = ""


class CreateUserForm(BaseModel):
    username: str
    password: str
    role: Literal["admin", "officer"] = "officer"


# ---------------------------------------------------------------------------
# Pydantic — Property operations
# ---------------------------------------------------------------------------

class PropertyRegistrationForm(BaseModel):
    property_key: str
    owner_name: str
    aadhar_no: str
    pan_no: str
    address: str
    pincode: str
    value: float
    survey_no: str
    rtc_no: Optional[str] = ""
    village: Optional[str] = ""
    taluk: Optional[str] = ""
    district: Optional[str] = ""
    state: Optional[str] = ""
    land_area: Optional[str] = ""
    land_type: Optional[str] = ""
    description: Optional[str] = ""

    @field_validator("value")
    @classmethod
    def value_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Property value must be a positive number.")
        return v


class PropertyTransferForm(BaseModel):
    property_key: str
    new_owner_name: str
    new_owner_aadhar: str
    new_owner_pan: str
    transfer_value: float
    stamp_duty_paid: Optional[float] = None
    registration_fee: Optional[float] = None

    @field_validator("transfer_value")
    @classmethod
    def transfer_value_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Transfer value must be a positive number.")
        return v


class PropertyInheritanceForm(BaseModel):
    property_key: str
    heir_name: str
    heir_aadhar: str
    heir_pan: str
    relationship: Optional[str] = ""
    legal_heir_certificate_no: Optional[str] = ""
