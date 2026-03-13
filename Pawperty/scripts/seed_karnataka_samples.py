from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any, Dict, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from auth import hash_password
from Blockchain import Owner, PropertyBlockchain
from database import Base, SessionLocal, engine
from models import Citizen, User


DATA_FILE = ROOT / "examples" / "karnataka_sample_data.json"


def load_dataset() -> Dict[str, Any]:
    return json.loads(DATA_FILE.read_text(encoding="utf-8"))


def make_owner(person: Dict[str, Any]) -> Owner:
    owner_name = person.get("name") or person.get("owner_name") or person.get("heir_name")
    if not owner_name:
        raise KeyError("Owner record must include 'name', 'owner_name', or 'heir_name'.")

    owner = Owner(owner_name, person["aadhar_no"], person["pan_no"])
    customer_key = person.get("customer_key", "").strip().upper()
    if customer_key:
        owner.customer_key = customer_key
    return owner


def ensure_tables() -> None:
    Base.metadata.create_all(bind=engine)


def ensure_default_staff() -> None:
    db = SessionLocal()
    try:
        if db.query(User).filter(User.username == "admin").first() is None:
            db.add(
                User(
                    username="admin",
                    hashed_password=hash_password("admin123"),
                    role="admin",
                    is_active=True,
                )
            )

        if db.query(User).filter(User.username == "officer_demo").first() is None:
            db.add(
                User(
                    username="officer_demo",
                    hashed_password=hash_password("officer123"),
                    role="officer",
                    is_active=True,
                )
            )

        db.commit()
    finally:
        db.close()


def ensure_citizens(citizens: Iterable[Dict[str, Any]]) -> int:
    db = SessionLocal()
    created_count = 0
    try:
        for citizen_data in citizens:
            customer_key = citizen_data["customer_key"].strip().upper()
            existing = db.query(Citizen).filter(Citizen.customer_key == customer_key).first()
            if existing is not None:
                continue

            db.add(
                Citizen(
                    customer_key=customer_key,
                    name=citizen_data["name"].strip().upper(),
                    aadhar_no=citizen_data["aadhar_no"].strip(),
                    pan_no=citizen_data["pan_no"].strip().upper(),
                    hashed_password=hash_password(citizen_data.get("password", "Demo@123")),
                    is_active=True,
                )
            )
            created_count += 1

        db.commit()
        return created_count
    finally:
        db.close()


def seed_properties(ledger: PropertyBlockchain, properties: Iterable[Dict[str, Any]]) -> int:
    created_count = 0
    for property_data in properties:
        property_key = property_data["property_key"].strip().upper()
        if property_key in ledger.property_index:
            continue

        owner = make_owner(property_data)
        ledger.add_property(
            property_key=property_key,
            owner=owner,
            address=property_data["address"],
            pincode=property_data["pincode"],
            value=float(property_data["value"]),
            survey_no=property_data["survey_no"],
            rtc_no=property_data.get("rtc_no", ""),
            village=property_data.get("village", ""),
            taluk=property_data.get("taluk", ""),
            district=property_data.get("district", ""),
            state=property_data.get("state", ""),
            land_area=property_data.get("land_area", ""),
            land_type=property_data.get("land_type", ""),
            description=property_data.get("description", ""),
            additional_info={"seed_tag": "karnataka_sample"},
        )
        created_count += 1
    return created_count


def has_operation(ledger: PropertyBlockchain, property_key: str, operation_id: str) -> bool:
    history = ledger.get_property_history(property_key)
    for record in history:
        additional_info = record.get("data", {}).get("additional_info", {})
        if additional_info.get("sample_operation_id") == operation_id:
            return True
    return False


def seed_transfers(ledger: PropertyBlockchain, transfers: Iterable[Dict[str, Any]]) -> int:
    created_count = 0
    for transfer_data in transfers:
        property_key = transfer_data["property_key"].strip().upper()
        operation_id = transfer_data["operation_id"].strip().upper()
        if property_key not in ledger.property_index or has_operation(ledger, property_key, operation_id):
            continue

        new_owner = make_owner(transfer_data["new_owner"])
        ledger.transfer_property(
            property_key=property_key,
            new_owner=new_owner,
            transfer_value=float(transfer_data["transfer_value"]),
            transfer_reason=transfer_data.get("transfer_reason", "sale"),
            stamp_duty_paid=float(transfer_data["stamp_duty_paid"]),
            registration_fee=float(transfer_data["registration_fee"]),
            additional_info={
                "sample_operation_id": operation_id,
                "seed_tag": "karnataka_sample",
                "notes": transfer_data.get("notes", ""),
            },
        )
        created_count += 1
    return created_count


def seed_inheritances(ledger: PropertyBlockchain, inheritances: Iterable[Dict[str, Any]]) -> int:
    created_count = 0
    for inheritance_data in inheritances:
        property_key = inheritance_data["property_key"].strip().upper()
        operation_id = inheritance_data["operation_id"].strip().upper()
        if property_key not in ledger.property_index or has_operation(ledger, property_key, operation_id):
            continue

        heir = make_owner(inheritance_data["heir"])
        ledger.inherit_property(
            property_key=property_key,
            heir=heir,
            relationship=inheritance_data.get("relationship", ""),
            legal_heir_certificate_no=inheritance_data.get("legal_heir_certificate_no", ""),
            additional_info={
                "sample_operation_id": operation_id,
                "seed_tag": "karnataka_sample",
                "notes": inheritance_data.get("notes", ""),
            },
        )
        created_count += 1
    return created_count


def main() -> None:
    dataset = load_dataset()
    ensure_tables()
    ensure_default_staff()

    citizens_created = ensure_citizens(dataset.get("citizens", []))

    ledger = PropertyBlockchain()
    properties_created = seed_properties(ledger, dataset.get("properties", []))
    transfers_created = seed_transfers(ledger, dataset.get("transfer_examples", []))
    inheritances_created = seed_inheritances(ledger, dataset.get("inheritance_examples", []))
    ledger.save_and_exit()

    print("Seed completed.")
    print(f"Citizens created: {citizens_created}")
    print(f"Properties created: {properties_created}")
    print(f"Transfers created: {transfers_created}")
    print(f"Inheritance records created: {inheritances_created}")
    print("Default users: admin/admin123 and officer_demo/officer123")


if __name__ == "__main__":
    main()