from __future__ import annotations

import importlib
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient


def _fresh_modules(tmp_path, monkeypatch):
    db_path = tmp_path / "pawperty-test.sqlite"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path.as_posix()}")

    for module_name in ["app", "auth", "database", "models"]:
        sys.modules.pop(module_name, None)

    database = importlib.import_module("database")
    models = importlib.import_module("models")
    auth = importlib.import_module("auth")
    app_module = importlib.import_module("app")

    ledger_path = tmp_path / "ledger-test.db"
    app_module.PropertyBlockchain.DB_FILE = str(ledger_path)
    return app_module, database, models, auth


def _login(client: TestClient, username: str, password: str) -> None:
    response = client.post(
        "/login",
        data={"username": username, "password": password},
        follow_redirects=False,
    )
    assert response.status_code == 302


def _seed_officer(database, models, auth_module, *, username: str = "officer1", password: str = "officer123"):
    db = database.SessionLocal()
    try:
        officer = models.User(
            username=username,
            hashed_password=auth_module.hash_password(password),
            role="officer",
            is_active=True,
        )
        db.add(officer)
        db.commit()
        db.refresh(officer)
        return officer
    finally:
        db.close()


def _seed_citizen(
    database,
    models,
    auth_module,
    *,
    customer_key: str,
    name: str,
    aadhar_no: str,
    pan_no: str,
    password: str = "citizen123",
):
    db = database.SessionLocal()
    try:
        citizen = models.Citizen(
            customer_key=customer_key,
            name=name,
            aadhar_no=aadhar_no,
            pan_no=pan_no,
            hashed_password=auth_module.hash_password(password),
            is_active=True,
        )
        db.add(citizen)
        db.commit()
        db.refresh(citizen)
        return citizen
    finally:
        db.close()


def _citizen_login(client: TestClient, customer_key: str, password: str) -> None:
    response = client.post(
        "/citizen/login",
        data={"customer_key": customer_key, "password": password},
        follow_redirects=False,
    )
    assert response.status_code == 302


def _add_property(ledger):
    from Blockchain import Owner

    ledger.add_property(
        property_key="PR1001",
        owner=Owner("Rajesh Kumar", "123456789012", "ABCDE1234F"),
        address="MG ROAD",
        pincode="560001",
        value=2500000,
        survey_no="SURV-1001",
        district="BENGALURU",
        state="KARNATAKA",
    )
    ledger._save_blockchain()


def test_blockchain_marks_original_transaction_as_corrected(tmp_path, monkeypatch):
    from Blockchain import Owner, PropertyBlockchain

    ledger_path = tmp_path / "unit-ledger.db"
    monkeypatch.setattr(PropertyBlockchain, "DB_FILE", str(ledger_path))
    ledger = PropertyBlockchain()

    ledger.add_property(
        property_key="PR2001",
        owner=Owner("Rajesh Kumar", "123456789012", "ABCDE1234F"),
        address="OLD ADDRESS",
        pincode="560001",
        value=1500000,
        survey_no="SURV-2001",
    )

    original_tx = ledger.get_property_history("PR2001")[0]["transaction_id"]
    ledger.create_correction_transaction(
        property_key="PR2001",
        original_transaction_id=original_tx,
        corrected_data={
            "owner_name": "Rakesh Kumar",
            "aadhar_no": "123456789012",
            "pan_no": "ABCDE1234F",
            "address": "NEW ADDRESS",
            "pincode": "560002",
            "value": 1750000,
        },
        correction_request_id="CRUNIT1",
        approved_by_authority="admin:reviewer",
    )

    history = ledger.get_property_history("PR2001")
    assert history[0]["status"] == ledger.STATUS_CORRECTED
    assert history[1]["data"]["type"] == "correction"
    assert history[1]["status"] == ledger.STATUS_VALID
    assert history[1]["data"]["previous_transaction_id"] == original_tx

    current_state = ledger.get_property_current_state("PR2001")
    assert current_state["owner"] == "RAKESH KUMAR"
    assert current_state["address"] == "NEW ADDRESS"
    assert current_state["status"] == ledger.STATUS_VALID


def test_blockchain_allows_duplicate_owner_names_for_distinct_identities(tmp_path, monkeypatch):
    from Blockchain import Owner, PropertyBlockchain

    ledger_path = tmp_path / "duplicate-name-ledger.db"
    monkeypatch.setattr(PropertyBlockchain, "DB_FILE", str(ledger_path))
    ledger = PropertyBlockchain()

    first_owner = Owner("Rajesh Kumar", "123456789012", "ABCDE1234F")
    second_owner = Owner("Rajesh Kumar", "999988887777", "ZXCVB4321K")

    ledger.add_property(
        property_key="PR3001",
        owner=first_owner,
        address="MG ROAD",
        pincode="560001",
        value=1500000,
        survey_no="SURV-3001",
    )
    ledger.add_property(
        property_key="PR3002",
        owner=second_owner,
        address="BRIGADE ROAD",
        pincode="560002",
        value=1750000,
        survey_no="SURV-3002",
    )

    first_state = ledger.get_property_current_state("PR3001")
    second_state = ledger.get_property_current_state("PR3002")

    assert first_state["owner"] == "RAJESH KUMAR"
    assert second_state["owner"] == "RAJESH KUMAR"
    assert first_state["customer_key"] != second_state["customer_key"]
    assert len(ledger.owner_registry) == 2


def test_past_properties_include_transferred_and_inherited_records(tmp_path, monkeypatch):
    from Blockchain import Owner, PropertyBlockchain

    ledger_path = tmp_path / "past-properties-ledger.db"
    monkeypatch.setattr(PropertyBlockchain, "DB_FILE", str(ledger_path))
    ledger = PropertyBlockchain()

    original_owner = Owner("Anita Rao", "123456789012", "ABCDE1234F")
    buyer = Owner("Kiran Das", "987654321098", "PQRSX6789L")
    heir = Owner("Maya Rao", "111122223333", "LMNOP4567Q")

    ledger.add_property(
        property_key="PR4001",
        owner=original_owner,
        address="MAIN ROAD",
        pincode="560001",
        value=2200000,
        survey_no="SURV-4001",
    )
    ledger.transfer_property("PR4001", buyer, transfer_value=2400000)

    ledger.add_property(
        property_key="PR4002",
        owner=original_owner,
        address="TEMPLE STREET",
        pincode="560003",
        value=1800000,
        survey_no="SURV-4002",
    )
    ledger.inherit_property("PR4002", heir, relationship="daughter")

    props = ledger.get_properties_by_customer_key(
        original_owner.customer_key,
        aadhar_no=original_owner.aadhar,
        pan_no=original_owner.pan,
        owner_name=original_owner.name,
    )

    assert props["current"] == []
    assert {prop["property_key"] for prop in props["past"]} == {"PR4001", "PR4002"}
    assert {prop["transfer_reason"] for prop in props["past"]} == {"SALE", "INHERITANCE"}


def test_admin_property_detail_shows_full_identity(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        ledger = app_module.app.state.ledger
        _add_property(ledger)

        _login(client, "admin", "admin123")
        response = client.get("/properties/PR1001")

        assert response.status_code == 200
        assert "123456789012" in response.text
        assert "ABCDE1234F" in response.text
        assert "1234****9012" not in response.text


def test_correction_submit_defaults_to_latest_valid_tx_when_blank(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        _seed_officer(database, models, auth_module)
        ledger = app_module.app.state.ledger
        _add_property(ledger)

        _login(client, "officer1", "officer123")
        submit_response = client.post(
            "/corrections/new",
            data={
                "property_key": "PR1001",
                "original_transaction_id": "",
                "error_description": "Address typo",
                "supporting_notes": "Should be MG ROAD EXTENSION.",
                "selected_fields": "address",
                "corrected_address": "MG ROAD EXTENSION",
            },
            follow_redirects=False,
        )

        assert submit_response.status_code == 302
        assert submit_response.headers["location"].startswith("/corrections/CR")


def test_correction_submit_infers_selected_fields_from_filled_values(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        _seed_officer(database, models, auth_module)
        ledger = app_module.app.state.ledger
        _add_property(ledger)
        original_tx = ledger.get_property_history("PR1001")[0]["transaction_id"]

        _login(client, "officer1", "officer123")
        submit_response = client.post(
            "/corrections/new",
            data={
                "property_key": "PR1001",
                "original_transaction_id": original_tx,
                "error_description": "Owner spelling correction",
                "supporting_notes": "Name should be Rakesh Kumar.",
                "corrected_owner_name": "Rakesh Kumar",
            },
            follow_redirects=False,
        )

        assert submit_response.status_code == 302
        request_path = submit_response.headers["location"]
        request_id = request_path.rsplit("/", 1)[-1]

        db = database.SessionLocal()
        try:
            correction = db.query(models.CorrectionRequest).filter_by(request_id=request_id).one()
            corrected_payload = json.loads(correction.corrected_data_json)
            assert corrected_payload["owner_name"] == "RAKESH KUMAR"
        finally:
            db.close()


def test_admin_approval_applies_correction_and_writes_audit(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        _seed_officer(database, models, auth_module)
        ledger = app_module.app.state.ledger
        _add_property(ledger)
        original_tx = ledger.get_property_history("PR1001")[0]["transaction_id"]

        _login(client, "officer1", "officer123")
        submit_response = client.post(
            "/corrections/new",
            data={
                "property_key": "PR1001",
                "original_transaction_id": original_tx,
                "error_description": "Wrong owner name entered during registration.",
                "supporting_notes": "Name on deed is Rakesh Kumar.",
                "selected_fields": "owner_name",
                "corrected_owner_name": "Rakesh Kumar",
            },
            follow_redirects=False,
        )
        assert submit_response.status_code == 302
        request_path = submit_response.headers["location"]
        request_id = request_path.rsplit("/", 1)[-1]

        client.post("/logout", follow_redirects=False)
        _login(client, "admin", "admin123")

        approve_response = client.post(
            f"/corrections/{request_id}/admin-approve",
            data={
                "owner_name": "Rakesh Kumar",
                "aadhar_no": "123456789012",
                "pan_no": "ABCDE1234F",
                "address": "MG ROAD",
                "pincode": "560001",
                "value": "2500000",
                "comment": "Verified deed and applied corrected owner name.",
            },
            follow_redirects=False,
        )
        assert approve_response.status_code == 302
        assert approve_response.headers["location"] == "/properties/PR1001"

        db = database.SessionLocal()
        try:
            correction = db.query(models.CorrectionRequest).filter_by(request_id=request_id).one()
            assert correction.status == "APPROVED"
            corrected_payload = json.loads(correction.corrected_data_json)
            assert corrected_payload["owner_name"] == "RAKESH KUMAR"

            activity = (
                db.query(models.UserBlockActivity)
                .filter_by(username="admin", action_type="APPLY_CORRECTION")
                .one()
            )
            assert activity.property_key == "PR1001"
            assert activity.block_index == 2
            assert activity.transaction_id == history_transaction_id_from_db(ledger, "PR1001", 1)

            audit_logs = (
                db.query(models.CorrectionAuditLog)
                .filter_by(correction_request_id=request_id)
                .order_by(models.CorrectionAuditLog.id.asc())
                .all()
            )
            assert [log.action_type for log in audit_logs] == ["SUBMITTED", "ADMIN_APPROVED_APPLIED"]
        finally:
            db.close()

        history = ledger.get_property_history("PR1001")
        assert len(history) == 2
        assert history[0]["status"] == ledger.STATUS_CORRECTED
        assert history[1]["data"]["type"] == "correction"
        assert history[1]["data"]["approved_by_authority"] == "admin:admin"


def test_corrections_list_filters_by_status_property_request_and_date(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        officer = _seed_officer(database, models, auth_module)
        now = datetime.now()

        db = database.SessionLocal()
        try:
            db.add_all(
                [
                    models.CorrectionRequest(
                        request_id="CRFILTER1",
                        property_key="PR1001",
                        original_transaction_id="TX1001",
                        error_description="Wrong PAN",
                        corrected_data_json=json.dumps({"owner_name": "ONE"}),
                        supporting_notes="",
                        submitted_officer_id=officer.id,
                        submitted_officer_name=officer.username,
                        status="APPROVED",
                        created_at=now - timedelta(days=1),
                        updated_at=now - timedelta(days=1),
                    ),
                    models.CorrectionRequest(
                        request_id="CRFILTER2",
                        property_key="PR9999",
                        original_transaction_id="TX9999",
                        error_description="Wrong owner",
                        corrected_data_json=json.dumps({}),
                        supporting_notes="",
                        submitted_officer_id=officer.id,
                        submitted_officer_name=officer.username,
                        status="REJECTED",
                        created_at=now - timedelta(days=10),
                        updated_at=now - timedelta(days=10),
                    ),
                ]
            )
            db.commit()
        finally:
            db.close()

        _login(client, "admin", "admin123")
        response = client.get(
            "/corrections",
            params={
                "status": "APPROVED",
                "property_key": "PR1001",
                "request_id": "CRFILTER1",
                "start_date": (now - timedelta(days=2)).strftime("%Y-%m-%d"),
                "end_date": now.strftime("%Y-%m-%d"),
            },
        )

        assert response.status_code == 200
        body = response.text
        assert "CRFILTER1" in body
        assert "PR1001" in body
        assert "CRFILTER2" not in body
        assert "PR9999" not in body


def history_transaction_id_from_db(ledger, property_key: str, index: int) -> str:
    return ledger.get_property_history(property_key)[index]["transaction_id"]


def test_register_action_creates_user_block_activity_and_admin_can_view_it(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        officer = _seed_officer(database, models, auth_module)

        _login(client, "officer1", "officer123")
        response = client.post(
            "/register",
            data={
                "property_key": "PR3001",
                "owner_name": "Mahesh Kumar",
                "aadhar_no": "111122223333",
                "pan_no": "PQRSX1234Z",
                "address": "BRIGADE ROAD",
                "pincode": "560025",
                "value": "3200000",
                "survey_no": "SURV-3001",
                "rtc_no": "",
                "village": "",
                "taluk": "",
                "district": "BENGALURU",
                "state": "KARNATAKA",
                "land_area": "",
                "land_type": "",
                "description": "",
            },
            follow_redirects=False,
        )
        assert response.status_code == 302
        assert response.headers["location"] == "/properties/PR3001"

        db = database.SessionLocal()
        try:
            activity = db.query(models.UserBlockActivity).filter_by(user_id=officer.id).one()
            assert activity.action_type == "REGISTER_PROPERTY"
            assert activity.block_index == 1
            assert activity.property_key == "PR3001"
        finally:
            db.close()

        client.post("/logout", follow_redirects=False)
        _login(client, "admin", "admin123")
        activity_page = client.get(f"/admin/users/{officer.id}/activity")
        assert activity_page.status_code == 200
        assert "REGISTER PROPERTY" in activity_page.text
        assert "PR3001" in activity_page.text
        assert "#1" in activity_page.text


def test_officer_can_open_request_correction_page(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        _seed_officer(database, models, auth_module)
        ledger = app_module.app.state.ledger
        _add_property(ledger)

        _login(client, "officer1", "officer123")
        response = client.get("/corrections/new", params={"property_key": "PR1001"})

        assert response.status_code == 200
        assert "Submit Correction Request" in response.text
        assert "PR1001" in response.text


def test_admin_users_page_renders_with_or_without_work_entries(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        _seed_officer(database, models, auth_module)
        _login(client, "admin", "admin123")

        response = client.get("/admin/users")

        assert response.status_code == 200
        assert "User Management" in response.text
        assert "Tracked Blocks" in response.text


def test_officer_can_open_users_overview_page(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        _seed_officer(database, models, auth_module)
        _login(client, "officer1", "officer123")

        response = client.get("/users")

        assert response.status_code == 200
        assert "Users Overview" in response.text
        assert "Citizen Users" in response.text


def test_users_overview_includes_citizens_with_only_past_properties(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        ledger = app_module.app.state.ledger
        from Blockchain import Owner

        original_owner = Owner("Nanda Rao", "123456789012", "ABCDE1234F")
        ledger.add_property(
            property_key="PR5001",
            owner=original_owner,
            address="MG ROAD",
            pincode="560001",
            value=2500000,
            survey_no="SURV-5001",
            district="BENGALURU",
            state="KARNATAKA",
        )
        ledger.transfer_property(
            property_key="PR5001",
            new_owner=Owner("Meera Das", "234567890123", "BCDEF2345G"),
            transfer_value=2600000,
        )
        ledger._save_blockchain()

        _seed_citizen(
            database,
            models,
            auth_module,
            customer_key=original_owner.customer_key,
            name="Nanda Rao",
            aadhar_no="123456789012",
            pan_no="ABCDE1234F",
        )

        _login(client, "admin", "admin123")
        response = client.get("/users")

        assert response.status_code == 200
        assert "Nanda Rao" in response.text
        assert ">0</td>" in response.text
        assert ">1</td>" in response.text


def test_users_overview_shows_view_action_for_citizens(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        _seed_officer(database, models, auth_module)
        _seed_citizen(
            database,
            models,
            auth_module,
            customer_key="CUST-TEST9001",
            name="View Citizen",
            aadhar_no="111122223333",
            pan_no="ASDFG1234H",
        )

        _login(client, "officer1", "officer123")
        response = client.get("/users")

        assert response.status_code == 200
        assert "/users/citizen/" in response.text
        assert "View" in response.text


def test_officer_can_open_users_citizen_detail(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        _seed_officer(database, models, auth_module)
        citizen = _seed_citizen(
            database,
            models,
            auth_module,
            customer_key="CUST-TEST9002",
            name="Detail Citizen",
            aadhar_no="222233334444",
            pan_no="QWERT1234Y",
        )

        _login(client, "officer1", "officer123")
        response = client.get(f"/users/citizen/{citizen.id}")

        assert response.status_code == 200
        assert "Citizen: Detail Citizen" in response.text
        assert "Current Properties" in response.text
        assert "Past Properties" in response.text


def test_officer_can_download_property_dataset_csv_without_user_data(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        _seed_officer(database, models, auth_module)
        ledger = app_module.app.state.ledger
        _add_property(ledger)

        _login(client, "officer1", "officer123")
        response = client.get("/exports/properties-dataset.csv")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/csv")
        assert "attachment; filename=" in response.headers.get("content-disposition", "")

        body = response.text
        assert "place,address,survey_no,land_type,land_area,village,taluk,district,state" in body
        assert "MG ROAD" in body
        assert "KARNATAKA" in body

        # Ensure user identity data is not exported.
        assert "owner" not in body.lower()
        assert "aadhar" not in body.lower()
        assert "pan" not in body.lower()


def test_citizen_dashboard_keeps_past_properties_after_transfer(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        ledger = app_module.app.state.ledger
        from Blockchain import Owner

        original_owner = Owner("Rajesh Kumar", "123456789012", "ABCDE1234F")
        ledger.add_property(
            property_key="PR4001",
            owner=original_owner,
            address="MG ROAD",
            pincode="560001",
            value=2500000,
            survey_no="SURV-4001",
            district="BENGALURU",
            state="KARNATAKA",
        )
        ledger.transfer_property(
            property_key="PR4001",
            new_owner=Owner("Meera Nair", "234567890123", "BCDEF2345G"),
            transfer_value=2750000,
        )
        ledger._save_blockchain()

        _seed_citizen(
            database,
            models,
            auth_module,
            customer_key="CK-CIT-4001",
            name="Rajesh Kumar",
            aadhar_no="123456789012",
            pan_no="ABCDE1234F",
        )

        _citizen_login(client, "CK-CIT-4001", "citizen123")
        response = client.get("/citizen/dashboard")

        assert response.status_code == 200
        assert "No properties currently registered under your ownership." in response.text
        assert "PR4001" in response.text
        assert "MEERA NAIR" in response.text


def test_citizen_dashboard_keeps_past_properties_after_corrected_owner_transfers(tmp_path, monkeypatch):
    app_module, database, models, auth_module = _fresh_modules(tmp_path, monkeypatch)

    with TestClient(app_module.app) as client:
        ledger = app_module.app.state.ledger
        from Blockchain import Owner

        registration = ledger.add_property(
            property_key="PR4002",
            owner=Owner("Rajesh Kumar", "123456789012", "ABCDE1234F"),
            address="MG ROAD",
            pincode="560001",
            value=2500000,
            survey_no="SURV-4002",
            district="BENGALURU",
            state="KARNATAKA",
        )
        ledger.transfer_property(
            property_key="PR4002",
            new_owner=Owner("Meera Nair", "234567890123", "BCDEF2345G"),
            transfer_value=2750000,
        )
        ledger.create_correction_transaction(
            property_key="PR4002",
            original_transaction_id=ledger.get_property_history("PR4002")[-1]["transaction_id"],
            corrected_data={
                "owner_name": "Ananya Murthy",
                "aadhar_no": "345678901234",
                "pan_no": "CDEFG3456H",
                "address": "MG ROAD",
                "pincode": "560001",
                "value": 2750000,
            },
            correction_request_id="CR-4002",
            approved_by_authority="AUTHORITY-1",
        )
        ledger.transfer_property(
            property_key="PR4002",
            new_owner=Owner("Naveen Kulkarni", "456789012345", "DEFGH4567J"),
            transfer_value=2900000,
        )
        ledger._save_blockchain()

        _seed_citizen(
            database,
            models,
            auth_module,
            customer_key="CK-CIT-4002",
            name="Ananya Murthy",
            aadhar_no="345678901234",
            pan_no="CDEFG3456H",
        )

        _citizen_login(client, "CK-CIT-4002", "citizen123")
        response = client.get("/citizen/dashboard")

        assert response.status_code == 200
        assert "No properties currently registered under your ownership." in response.text
        assert "PR4002" in response.text
        assert "NAVEEN KULKARNI" in response.text