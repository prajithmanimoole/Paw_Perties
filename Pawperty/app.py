"""Pawperty — FastAPI application entry point.

Roles
-----
admin   : blockchain management + all property operations + user management
officer : property registration, transfer, inheritance, view, search
citizen : view own properties and ownership history (read-only)
"""

from __future__ import annotations

import os
import json
import uuid
import csv
import io
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import Depends, FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func
from sqlalchemy.orm import Session
from starlette.middleware.sessions import SessionMiddleware

from auth import (
    NotAuthenticatedException,
    clear_session,
    get_current_citizen,
    get_current_user,
    hash_password,
    require_admin,
    require_citizen,
    require_officer,
    require_officer_or_admin,
    set_session_citizen,
    set_session_user,
    verify_password,
)
from Blockchain import Owner, PropertyBlockchain
from database import Base, SessionLocal, engine, get_db
from models import Citizen, CorrectionAuditLog, CorrectionRequest, User, UserBlockActivity


# ---------------------------------------------------------------------------
# Lifespan: DB init + blockchain load + seed admin
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create the users table if it doesn't exist
    Base.metadata.create_all(bind=engine)

    # Seed the default admin on first boot
    db = SessionLocal()
    try:
        if db.query(User).count() == 0:
            admin = User(
                username="admin",
                hashed_password=hash_password("admin123"),
                role="admin",
                is_active=True,
            )
            db.add(admin)
            db.commit()
            print("=" * 60)
            print("  DEFAULT ADMIN CREATED")
            print("  Username : admin")
            print("  Password : admin123")
            print("  !! Change this password immediately !!")
            print("=" * 60)
    finally:
        db.close()

    # Load / initialise blockchain
    app.state.ledger = PropertyBlockchain()

    yield

    # Shutdown: persist blockchain
    app.state.ledger.save_and_exit()


# ---------------------------------------------------------------------------
# App configuration
# ---------------------------------------------------------------------------

SECRET_KEY = os.getenv("SECRET_KEY", "pawperty-secret-key-change-in-production")

app = FastAPI(title="Pawperty", lifespan=lifespan)

app.add_middleware(
    SessionMiddleware,
    secret_key=SECRET_KEY,
    session_cookie="pawperty_session",
    max_age=86400,
    https_only=False,
)

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


# ---------------------------------------------------------------------------
# Global exception handlers
# ---------------------------------------------------------------------------

@app.exception_handler(NotAuthenticatedException)
async def unauthenticated_handler(request: Request, exc: NotAuthenticatedException):
    """Redirect any unauthenticated request to the login page."""
    if request.headers.get("HX-Request") == "true":
        response = HTMLResponse(content="", status_code=200)
        response.headers["HX-Redirect"] = "/login"
        return response
    return RedirectResponse(url="/login", status_code=302)


# ---------------------------------------------------------------------------
# Template context helper
# ---------------------------------------------------------------------------

def _ctx(request: Request, db: Session, **kwargs: Any) -> Dict[str, Any]:
    """Build a base template context enriched with the current user or citizen."""
    user = get_current_user(request, db)
    citizen = get_current_citizen(request, db) if user is None else None
    return {"request": request, "current_user": user, "current_citizen": citizen, **kwargs}


def _ledger(request: Request) -> PropertyBlockchain:
    return request.app.state.ledger


def _flash(request: Request, message: str, category: str = "success") -> None:
    request.session["flash"] = {"message": message, "category": category}


def _pop_flash(request: Request) -> Optional[Dict[str, str]]:
    return request.session.pop("flash", None)


def _new_correction_request_id() -> str:
    return f"CR{str(uuid.uuid4()).upper().replace('-', '')[:8]}"


def _normalize_correction_payload(
    ledger: PropertyBlockchain,
    owner_name: str,
    aadhar_no: str,
    pan_no: str,
    address: str,
    pincode: str,
    value: str,
) -> Dict[str, Any]:
    clean_owner = owner_name.strip().upper()
    clean_aadhar = aadhar_no.replace(" ", "").replace("-", "")
    clean_pan = pan_no.strip().upper()
    clean_address = address.strip().upper()
    clean_pincode = pincode.strip().upper()

    if not clean_owner:
        raise ValueError("Owner name cannot be empty.")
    if not ledger.validate_aadhar(clean_aadhar):
        raise ValueError("Aadhar must be exactly 12 digits.")
    if not ledger.validate_pan(clean_pan):
        raise ValueError("PAN must be in format ABCDE1234F.")

    corrected_value = float(value)
    if corrected_value <= 0:
        raise ValueError("Property value must be a positive number.")

    return {
        "owner_name": clean_owner,
        "aadhar_no": clean_aadhar,
        "pan_no": clean_pan,
        "address": clean_address,
        "pincode": clean_pincode,
        "value": corrected_value,
    }


def _build_requested_corrections(
    ledger: PropertyBlockchain,
    selected_fields: List[str],
    corrected_owner_name: str,
    corrected_aadhar_no: str,
    corrected_pan_no: str,
    corrected_address: str,
    corrected_pincode: str,
    corrected_value: str,
) -> Dict[str, Any]:
    allowed_fields = {"owner_name", "aadhar_no", "pan_no", "address", "pincode", "value"}
    requested_fields = {field.strip() for field in selected_fields if field.strip()}

    if not requested_fields:
        raise ValueError("Select at least one field that needs correction.")

    invalid_fields = requested_fields - allowed_fields
    if invalid_fields:
        raise ValueError("Invalid correction field selection.")

    payload: Dict[str, Any] = {}

    if "owner_name" in requested_fields:
        owner = corrected_owner_name.strip().upper()
        if not owner:
            raise ValueError("Correct owner name is required.")
        payload["owner_name"] = owner

    if "aadhar_no" in requested_fields:
        aadhar = corrected_aadhar_no.replace(" ", "").replace("-", "")
        if not ledger.validate_aadhar(aadhar):
            raise ValueError("Correct Aadhaar must be exactly 12 digits.")
        payload["aadhar_no"] = aadhar

    if "pan_no" in requested_fields:
        pan = corrected_pan_no.strip().upper()
        if not ledger.validate_pan(pan):
            raise ValueError("Correct PAN must be in format ABCDE1234F.")
        payload["pan_no"] = pan

    if "address" in requested_fields:
        address = corrected_address.strip().upper()
        if not address:
            raise ValueError("Correct address is required.")
        payload["address"] = address

    if "pincode" in requested_fields:
        pincode = corrected_pincode.strip().upper()
        if not pincode:
            raise ValueError("Correct pincode is required.")
        payload["pincode"] = pincode

    if "value" in requested_fields:
        try:
            value = float(corrected_value)
        except ValueError as exc:
            raise ValueError("Correct value must be a valid number.") from exc
        if value <= 0:
            raise ValueError("Correct value must be greater than zero.")
        payload["value"] = value

    return payload


def _latest_valid_transaction_id(ledger: PropertyBlockchain, property_key: str) -> str:
    """Return the latest VALID transaction id for a property."""
    history = ledger.get_property_history(property_key)
    for record in reversed(history):
        if record.get("status") == ledger.STATUS_VALID:
            return str(record.get("transaction_id", "")).strip().upper()
    raise ValueError("No VALID transaction found for this property.")


def _infer_selected_fields(
    selected_fields: List[str],
    corrected_owner_name: str,
    corrected_aadhar_no: str,
    corrected_pan_no: str,
    corrected_address: str,
    corrected_pincode: str,
    corrected_value: str,
) -> List[str]:
    """Infer correction fields from non-empty corrected inputs when checkboxes are missed."""
    normalized = [field.strip() for field in selected_fields if field and field.strip()]
    if normalized:
        return normalized

    inferred: List[str] = []
    if corrected_owner_name.strip():
        inferred.append("owner_name")
    if corrected_aadhar_no.replace(" ", "").replace("-", ""):
        inferred.append("aadhar_no")
    if corrected_pan_no.strip():
        inferred.append("pan_no")
    if corrected_address.strip():
        inferred.append("address")
    if corrected_pincode.strip():
        inferred.append("pincode")
    if corrected_value.strip():
        inferred.append("value")

    return inferred


def _append_correction_audit(
    db: Session,
    correction_request_id: str,
    actor: User,
    action_type: str,
    comments: str = "",
) -> None:
    db.add(
        CorrectionAuditLog(
            correction_request_id=correction_request_id,
            actor_user_id=actor.id,
            actor_username=actor.username,
            actor_role=actor.role,
            action_type=action_type,
            comments=comments,
        )
    )


def _record_block_activity(db: Session, actor: User, block: Any, action_type: str) -> None:
    db.add(
        UserBlockActivity(
            user_id=actor.id,
            username=actor.username,
            user_role=actor.role,
            action_type=action_type,
            property_key=block.property_key,
            block_index=block.index,
            transaction_id=block.data.get("transaction_id", ""),
        )
    )


def _activity_summary_by_user(db: Session) -> Dict[int, int]:
    rows = (
        db.query(UserBlockActivity.user_id, func.count(UserBlockActivity.id))
        .group_by(UserBlockActivity.user_id)
        .all()
    )
    return {user_id: count for user_id, count in rows}


def _build_citizen_user_rows(db: Session, ledger: PropertyBlockchain) -> List[Dict[str, Any]]:
    """Build citizen rows enriched with current/past property ownership counts."""
    citizens = db.query(Citizen).order_by(Citizen.created_at.asc()).all()
    rows: List[Dict[str, Any]] = []

    for citizen in citizens:
        properties = ledger.get_properties_by_customer_key(
            citizen.customer_key,
            aadhar_no=citizen.aadhar_no,
            pan_no=citizen.pan_no,
            owner_name=citizen.name,
        )
        current_props = properties.get("current", [])
        past_props = properties.get("past", [])

        rows.append(
            {
                "id": citizen.id,
                "name": citizen.name,
                "customer_key": citizen.customer_key,
                "aadhar_no": citizen.aadhar_no,
                "pan_no": citizen.pan_no,
                "is_active": citizen.is_active,
                "created_at": citizen.created_at,
                "current_count": len(current_props),
                "past_count": len(past_props),
            }
        )

    return rows


def _serialize_correction_request(req: CorrectionRequest) -> Dict[str, Any]:
    return {
        "request_id": req.request_id,
        "property_key": req.property_key,
        "original_transaction_id": req.original_transaction_id,
        "error_description": req.error_description,
        "corrected_data": json.loads(req.corrected_data_json or "{}"),
        "supporting_notes": req.supporting_notes,
        "submitted_officer_name": req.submitted_officer_name,
        "status": req.status,
        "created_at": req.created_at,
        "updated_at": req.updated_at,
    }


def _parse_filter_date(raw_value: str, *, end_of_day: bool = False) -> Optional[datetime]:
    value = raw_value.strip()
    if not value:
        return None
    parsed = datetime.strptime(value, "%Y-%m-%d")
    if end_of_day:
        return parsed + timedelta(days=1)
    return parsed


# ---------------------------------------------------------------------------
# Auth routes
# ---------------------------------------------------------------------------

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, tab: str = "staff", db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if user:
        return RedirectResponse("/", status_code=302)
    citizen = get_current_citizen(request, db)
    if citizen:
        return RedirectResponse("/citizen/dashboard", status_code=302)
    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "login.html", {"request": request, "current_user": None, "current_citizen": None, "flash": flash, "active_tab": tab}
    )


@app.post("/login")
async def login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    user = db.query(User).filter(User.username == username).first()

    if user is None or not verify_password(password, user.hashed_password):
        _flash(request, "Invalid username or password.", "error")
        return RedirectResponse("/login", status_code=302)

    if not user.is_active:
        _flash(request, "Your account has been suspended. Contact an administrator.", "error")
        return RedirectResponse("/login", status_code=302)

    set_session_user(request, user)
    return RedirectResponse("/", status_code=302)


@app.post("/logout")
async def logout(request: Request):
    clear_session(request)
    return RedirectResponse("/login", status_code=302)


# ---------------------------------------------------------------------------
# Citizen Auth & Dashboard
# ---------------------------------------------------------------------------

@app.post("/citizen/login")
async def citizen_login_post(
    request: Request,
    customer_key: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    citizen = db.query(Citizen).filter(Citizen.customer_key == customer_key.strip().upper()).first()

    if citizen is None or not verify_password(password, citizen.hashed_password):
        _flash(request, "Invalid Customer ID or password.", "error")
        return RedirectResponse("/login?tab=citizen", status_code=302)

    if not citizen.is_active:
        _flash(request, "Your account has been suspended. Contact an administrator.", "error")
        return RedirectResponse("/login?tab=citizen", status_code=302)

    set_session_citizen(request, citizen)
    return RedirectResponse("/citizen/dashboard", status_code=302)


@app.get("/citizen/register", response_class=HTMLResponse)
async def citizen_register_page(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if user:
        return RedirectResponse("/", status_code=302)
    citizen = get_current_citizen(request, db)
    if citizen:
        return RedirectResponse("/citizen/dashboard", status_code=302)
    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "citizen/register.html",
        {"request": request, "current_user": None, "current_citizen": None, "flash": flash, "form_data": {}},
    )


@app.post("/citizen/register")
async def citizen_register_post(
    request: Request,
    name: str = Form(...),
    aadhar_no: str = Form(...),
    pan_no: str = Form(...),
    password: str = Form(...),
    customer_key: str = Form(""),
    db: Session = Depends(get_db),
):
    ledger = _ledger(request)
    form_data = {"name": name, "aadhar_no": aadhar_no, "pan_no": pan_no, "customer_key": customer_key}
    aadhar_clean = aadhar_no.replace(" ", "").replace("-", "")
    pan_clean = pan_no.strip().upper()
    name_clean = name.strip().upper()

    # Basic validation
    if not ledger.validate_aadhar(aadhar_clean):
        _flash(request, "Invalid Aadhar number. Must be 12 digits.", "error")
        return templates.TemplateResponse(
            "citizen/register.html",
            {"request": request, "current_user": None, "current_citizen": None,
             "flash": {"message": "Invalid Aadhar number. Must be 12 digits.", "category": "error"}, "form_data": form_data},
            status_code=422,
        )

    if not ledger.validate_pan(pan_clean):
        _flash(request, "Invalid PAN number. Must be in format ABCDE1234F.", "error")
        return templates.TemplateResponse(
            "citizen/register.html",
            {"request": request, "current_user": None, "current_citizen": None,
             "flash": {"message": "Invalid PAN number. Must be in format ABCDE1234F.", "category": "error"}, "form_data": form_data},
            status_code=422,
        )

    # Check if Aadhar already registered as citizen
    existing = db.query(Citizen).filter(Citizen.aadhar_no == aadhar_clean).first()
    if existing:
        return templates.TemplateResponse(
            "citizen/register.html",
            {"request": request, "current_user": None, "current_citizen": None,
             "flash": {"message": "An account with this Aadhar number already exists. Please login instead.", "category": "error"}, "form_data": form_data},
            status_code=422,
        )

    # If customer_key provided, verify it exists in blockchain and Aadhar matches
    cust_key = customer_key.strip().upper()
    if cust_key:
        owner_info = ledger.get_owner_by_customer_key(cust_key)
        if owner_info is None:
            return templates.TemplateResponse(
                "citizen/register.html",
                {"request": request, "current_user": None, "current_citizen": None,
                 "flash": {"message": f"Customer ID '{cust_key}' not found in the system.", "category": "error"}, "form_data": form_data},
                status_code=422,
            )
        if owner_info["aadhar"] != aadhar_clean:
            return templates.TemplateResponse(
                "citizen/register.html",
                {"request": request, "current_user": None, "current_citizen": None,
                 "flash": {"message": "Aadhar number does not match the Customer ID on record.", "category": "error"}, "form_data": form_data},
                status_code=422,
            )
    else:
        # Generate a new customer key
        import uuid
        unique_id = str(uuid.uuid4()).upper().replace('-', '')[:8]
        cust_key = f"CUST-{unique_id}"

    new_citizen = Citizen(
        customer_key=cust_key,
        name=name_clean,
        aadhar_no=aadhar_clean,
        pan_no=pan_clean,
        hashed_password=hash_password(password),
        is_active=True,
    )
    db.add(new_citizen)
    db.commit()
    db.refresh(new_citizen)

    set_session_citizen(request, new_citizen)
    _flash(request, f"Welcome, {name_clean}! Your Customer ID is {cust_key}.")
    return RedirectResponse("/citizen/dashboard", status_code=302)


@app.get("/citizen/dashboard", response_class=HTMLResponse)
async def citizen_dashboard(
    request: Request,
    db: Session = Depends(get_db),
    citizen: Citizen = Depends(require_citizen),
):
    ledger = _ledger(request)
    props = ledger.get_properties_by_customer_key(
        citizen.customer_key,
        aadhar_no=citizen.aadhar_no,
        pan_no=citizen.pan_no,
        owner_name=citizen.name,
    )
    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "citizen/dashboard.html",
        _ctx(
            request,
            db,
            current_properties=props["current"],
            past_properties=props["past"],
            flash=flash,
        ),
    )


@app.get("/citizen/property/{key}", response_class=HTMLResponse)
async def citizen_property_detail(
    key: str,
    request: Request,
    db: Session = Depends(get_db),
    citizen: Citizen = Depends(require_citizen),
):
    ledger = _ledger(request)
    try:
        state = ledger.get_property_current_state(key)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Property '{key}' not found.")

    # Verify citizen has a relationship with this property
    props = ledger.get_properties_by_customer_key(
        citizen.customer_key,
        aadhar_no=citizen.aadhar_no,
        pan_no=citizen.pan_no,
        owner_name=citizen.name,
    )
    all_keys = [p["property_key"] for p in props["current"] + props["past"]]
    if key not in all_keys:
        raise HTTPException(status_code=403, detail="You do not have access to this property.")

    history = ledger.get_property_history(key)
    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "citizen/property_detail.html",
        _ctx(request, db, property=state, history=history, flash=flash),
    )


@app.post("/citizen/logout")
async def citizen_logout(request: Request):
    clear_session(request)
    return RedirectResponse("/login?tab=citizen", status_code=302)


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)
    all_props = ledger.get_all_properties()
    recent = sorted(all_props, key=lambda p: p.get("last_updated", ""), reverse=True)[:5]
    flash = _pop_flash(request)
    recent_staff_work = []
    work_totals = []

    if user.role == "admin":
        recent_staff_work = (
            db.query(UserBlockActivity)
            .order_by(UserBlockActivity.created_at.desc(), UserBlockActivity.id.desc())
            .limit(10)
            .all()
        )
        work_totals = (
            db.query(
                UserBlockActivity.user_id,
                UserBlockActivity.username,
                UserBlockActivity.user_role,
                func.count(UserBlockActivity.id).label("block_count"),
            )
            .group_by(
                UserBlockActivity.user_id,
                UserBlockActivity.username,
                UserBlockActivity.user_role,
            )
            .order_by(func.count(UserBlockActivity.id).desc(), UserBlockActivity.username.asc())
            .all()
        )

    return templates.TemplateResponse(
        "index.html",
        _ctx(
            request,
            db,
            block_count=len(ledger.chain),
            property_count=len(all_props),
            recent_properties=recent,
            recent_staff_work=recent_staff_work,
            work_totals=work_totals,
            flash=flash,
        ),
    )


# ---------------------------------------------------------------------------
# Properties — view
# ---------------------------------------------------------------------------

@app.get("/analytics", response_class=HTMLResponse)
async def analytics_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer),
):
    import json as _json
    ledger = _ledger(request)
    data = ledger.get_analytics_data()
    return templates.TemplateResponse(
        "analytics.html",
        _ctx(
            request,
            db,
            timeline_json=_json.dumps(data["timeline"]),
            heatmap=data["heatmap"],
            total_properties=len(data["locations"]),
            total_districts=len(data["heatmap"]),
        ),
    )


@app.get("/properties", response_class=HTMLResponse)
async def properties_list(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)
    all_props = ledger.get_all_properties()
    return templates.TemplateResponse(
        "properties.html", _ctx(request, db, properties=all_props)
    )


@app.get("/properties/export.csv")
@app.get("/properties/export")
@app.get("/exports/properties-dataset.csv")
async def properties_dataset_export_csv(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer),
):
    """Export non-user property dataset fields for analytics/model training."""
    ledger = _ledger(request)
    all_props = ledger.get_all_properties()

    output = io.StringIO()
    fieldnames = [
        "place",
        "address",
        "survey_no",
        "land_type",
        "land_area",
        "village",
        "taluk",
        "district",
        "state",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for prop in all_props:
        location = prop.get("location", {}) or {}
        land = prop.get("land_details", {}) or {}
        writer.writerow(
            {
                "place": prop.get("address", ""),
                "address": prop.get("address", ""),
                "survey_no": prop.get("survey_no", ""),
                "land_type": land.get("type", ""),
                "land_area": land.get("area", ""),
                "village": location.get("village", ""),
                "taluk": location.get("taluk", ""),
                "district": location.get("district", ""),
                "state": location.get("state", ""),
            }
        )

    csv_data = output.getvalue()
    output.close()
    filename = f"properties_dataset_{datetime.now().strftime('%Y%m%d')}.csv"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=csv_data, media_type="text/csv", headers=headers)


@app.get("/properties/{key}", response_class=HTMLResponse)
async def property_detail(
    key: str,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)
    try:
        state = ledger.get_property_current_state(key)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Property '{key}' not found.")

    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "property_detail.html", _ctx(request, db, property=state, flash=flash)
    )


# ---------------------------------------------------------------------------
# Register property
# ---------------------------------------------------------------------------

@app.get("/register", response_class=HTMLResponse)
async def register_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "register.html", _ctx(request, db, flash=flash, form_data={})
    )


@app.post("/register")
async def register_post(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
    property_key: str = Form(...),
    owner_name: str = Form(...),
    aadhar_no: str = Form(...),
    pan_no: str = Form(...),
    address: str = Form(...),
    pincode: str = Form(...),
    value: float = Form(...),
    survey_no: str = Form(...),
    rtc_no: str = Form(""),
    village: str = Form(""),
    taluk: str = Form(""),
    district: str = Form(""),
    state: str = Form(""),
    land_area: str = Form(""),
    land_type: str = Form(""),
    description: str = Form(""),
):
    ledger = _ledger(request)
    # Preserve form data for re-render on error
    form_data = {
        "property_key": property_key, "owner_name": owner_name,
        "aadhar_no": aadhar_no, "pan_no": pan_no, "address": address,
        "pincode": pincode, "value": value, "survey_no": survey_no,
        "rtc_no": rtc_no, "village": village, "taluk": taluk,
        "district": district, "state": state, "land_area": land_area,
        "land_type": land_type, "description": description,
    }
    try:
        owner = Owner(owner_name, aadhar_no, pan_no)
        ledger.add_property(
            property_key=property_key,
            owner=owner,
            address=address,
            pincode=pincode,
            value=value,
            survey_no=survey_no,
            rtc_no=rtc_no,
            village=village,
            taluk=taluk,
            district=district,
            state=state,
            land_area=land_area,
            land_type=land_type,
            description=description,
        )
        _record_block_activity(db, user, ledger.get_latest_block(), "REGISTER_PROPERTY")
        db.commit()
        ledger._save_blockchain()
        _flash(request, f"Property '{property_key}' registered successfully.")
        return RedirectResponse(f"/properties/{property_key}", status_code=302)
    except ValueError as exc:
        return templates.TemplateResponse(
            "register.html",
            _ctx(request, db, flash={"message": str(exc), "category": "error"}, form_data=form_data),
            status_code=422,
        )


# ---------------------------------------------------------------------------
# Transfer property
# ---------------------------------------------------------------------------

@app.get("/transfer", response_class=HTMLResponse)
async def transfer_page(
    request: Request,
    key: Optional[str] = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)
    prefill_state = None
    if key and key in ledger.property_index:
        prefill_state = ledger.get_property_current_state(key)

    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "transfer.html",
        _ctx(request, db, flash=flash, form_data={}, prefill_state=prefill_state, prefill_key=key or ""),
    )


@app.post("/transfer")
async def transfer_post(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
    property_key: str = Form(...),
    new_owner_name: str = Form(...),
    new_owner_aadhar: str = Form(...),
    new_owner_pan: str = Form(...),
    transfer_value: float = Form(...),
    stamp_duty_paid: Optional[float] = Form(None),
    registration_fee: Optional[float] = Form(None),
):
    ledger = _ledger(request)
    form_data = {
        "property_key": property_key, "new_owner_name": new_owner_name,
        "new_owner_aadhar": new_owner_aadhar, "new_owner_pan": new_owner_pan,
        "transfer_value": transfer_value, "stamp_duty_paid": stamp_duty_paid,
        "registration_fee": registration_fee,
    }

    try:
        new_owner = Owner(new_owner_name, new_owner_aadhar, new_owner_pan)
        ledger.transfer_property(
            property_key=property_key,
            new_owner=new_owner,
            transfer_value=transfer_value,
            stamp_duty_paid=stamp_duty_paid,
            registration_fee=registration_fee,
        )
        _record_block_activity(db, user, ledger.get_latest_block(), "TRANSFER_PROPERTY")
        db.commit()
        ledger._save_blockchain()
        _flash(request, f"Property '{property_key}' transferred successfully.")
        return RedirectResponse(f"/properties/{property_key}", status_code=302)
    except ValueError as exc:
        prefill_state = None
        if property_key in ledger.property_index:
            prefill_state = ledger.get_property_current_state(property_key)
        return templates.TemplateResponse(
            "transfer.html",
            _ctx(
                request, db,
                flash={"message": str(exc), "category": "error"},
                form_data=form_data,
                prefill_state=prefill_state,
                prefill_key=property_key,
            ),
            status_code=422,
        )


# ---------------------------------------------------------------------------
# Inherit property
# ---------------------------------------------------------------------------

@app.get("/inherit", response_class=HTMLResponse)
async def inherit_page(
    request: Request,
    key: Optional[str] = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)
    prefill_state = None
    if key and key in ledger.property_index:
        prefill_state = ledger.get_property_current_state(key)

    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "inherit.html",
        _ctx(request, db, flash=flash, form_data={}, prefill_state=prefill_state, prefill_key=key or ""),
    )


@app.post("/inherit")
async def inherit_post(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
    property_key: str = Form(...),
    heir_name: str = Form(...),
    heir_aadhar: str = Form(...),
    heir_pan: str = Form(...),
    relationship: str = Form(""),
    legal_heir_certificate_no: str = Form(""),
):
    ledger = _ledger(request)
    form_data = {
        "property_key": property_key, "heir_name": heir_name,
        "heir_aadhar": heir_aadhar, "heir_pan": heir_pan,
        "relationship": relationship,
        "legal_heir_certificate_no": legal_heir_certificate_no,
    }

    try:
        heir = Owner(heir_name, heir_aadhar, heir_pan)
        ledger.inherit_property(
            property_key=property_key,
            heir=heir,
            relationship=relationship,
            legal_heir_certificate_no=legal_heir_certificate_no,
        )
        _record_block_activity(db, user, ledger.get_latest_block(), "INHERIT_PROPERTY")
        db.commit()
        ledger._save_blockchain()
        _flash(request, f"Property '{property_key}' inherited successfully.")
        return RedirectResponse(f"/properties/{property_key}", status_code=302)
    except ValueError as exc:
        prefill_state = None
        if property_key in ledger.property_index:
            prefill_state = ledger.get_property_current_state(property_key)
        return templates.TemplateResponse(
            "inherit.html",
            _ctx(
                request, db,
                flash={"message": str(exc), "category": "error"},
                form_data=form_data,
                prefill_state=prefill_state,
                prefill_key=property_key,
            ),
            status_code=422,
        )


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@app.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    return templates.TemplateResponse("search.html", _ctx(request, db))


# ---------------------------------------------------------------------------
# Corrections workflow
# ---------------------------------------------------------------------------

@app.get("/corrections", response_class=HTMLResponse)
async def corrections_list(
    request: Request,
    status: str = "",
    property_key: str = "",
    request_id: str = "",
    start_date: str = "",
    end_date: str = "",
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    query = db.query(CorrectionRequest)
    if user.role == "officer":
        query = query.filter(CorrectionRequest.submitted_officer_id == user.id)

    status_value = status.strip().upper()
    property_value = property_key.strip().upper()
    request_value = request_id.strip().upper()

    if status_value:
        query = query.filter(CorrectionRequest.status == status_value)
    if property_value:
        query = query.filter(CorrectionRequest.property_key.contains(property_value))
    if request_value:
        query = query.filter(CorrectionRequest.request_id.contains(request_value))

    try:
        start_dt = _parse_filter_date(start_date)
        end_dt = _parse_filter_date(end_date, end_of_day=True)
    except ValueError:
        flash = {"message": "Date filters must use YYYY-MM-DD.", "category": "error"}
        return templates.TemplateResponse(
            "corrections.html",
            _ctx(
                request,
                db,
                correction_requests=[],
                filter_values={
                    "status": status,
                    "property_key": property_key,
                    "request_id": request_id,
                    "start_date": start_date,
                    "end_date": end_date,
                },
                flash=flash,
            ),
            status_code=422,
        )

    if start_dt is not None:
        query = query.filter(CorrectionRequest.created_at >= start_dt)
    if end_dt is not None:
        query = query.filter(CorrectionRequest.created_at < end_dt)

    requests = query.order_by(CorrectionRequest.created_at.desc()).all()

    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "corrections.html",
        _ctx(
            request,
            db,
            correction_requests=[_serialize_correction_request(req) for req in requests],
            filter_values={
                "status": status,
                "property_key": property_key,
                "request_id": request_id,
                "start_date": start_date,
                "end_date": end_date,
            },
            flash=flash,
        ),
    )


@app.get("/corrections/new", response_class=HTMLResponse)
async def correction_new_page(
    request: Request,
    property_key: Optional[str] = None,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer),
):
    ledger = _ledger(request)
    history = []
    current_state = None
    form_data: Dict[str, Any] = {
        "property_key": property_key or "",
        "selected_fields": [],
        "corrected_owner_name": "",
        "corrected_aadhar_no": "",
        "corrected_pan_no": "",
        "corrected_address": "",
        "corrected_pincode": "",
        "corrected_value": "",
    }
    if property_key and property_key in ledger.property_index:
        history = ledger.get_property_history(property_key)
        current_state = ledger.get_property_current_state(property_key)
        latest_valid_tx_id = _latest_valid_transaction_id(ledger, property_key)
        form_data.update(
            {
                "original_transaction_id": latest_valid_tx_id,
                "corrected_owner_name": current_state.get("owner", ""),
                "corrected_aadhar_no": current_state.get("aadhar_no", ""),
                "corrected_pan_no": current_state.get("pan_no", ""),
                "corrected_address": current_state.get("address", ""),
                "corrected_pincode": current_state.get("pincode", ""),
                "corrected_value": str(current_state.get("value", "")),
            }
        )

    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "correction_new.html",
        _ctx(
            request,
            db,
            flash=flash,
            form_data=form_data,
            property_history=history,
            current_state=current_state,
        ),
    )


@app.post("/corrections/new")
async def correction_new_post(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer),
    property_key: str = Form(...),
    original_transaction_id: str = Form(""),
    error_description: str = Form(...),
    supporting_notes: str = Form(""),
    selected_fields: List[str] = Form([]),
    corrected_owner_name: str = Form(""),
    corrected_aadhar_no: str = Form(""),
    corrected_pan_no: str = Form(""),
    corrected_address: str = Form(""),
    corrected_pincode: str = Form(""),
    corrected_value: str = Form(""),
):
    ledger = _ledger(request)

    property_key_clean = property_key.strip().upper()
    selected_fields_normalized = _infer_selected_fields(
        selected_fields,
        corrected_owner_name=corrected_owner_name,
        corrected_aadhar_no=corrected_aadhar_no,
        corrected_pan_no=corrected_pan_no,
        corrected_address=corrected_address,
        corrected_pincode=corrected_pincode,
        corrected_value=corrected_value,
    )

    tx_id_clean = original_transaction_id.strip().upper()
    if not tx_id_clean and property_key_clean in ledger.property_index:
        tx_id_clean = _latest_valid_transaction_id(ledger, property_key_clean)

    form_data = {
        "property_key": property_key_clean,
        "original_transaction_id": tx_id_clean,
        "error_description": error_description,
        "supporting_notes": supporting_notes,
        "selected_fields": selected_fields_normalized,
        "corrected_owner_name": corrected_owner_name,
        "corrected_aadhar_no": corrected_aadhar_no,
        "corrected_pan_no": corrected_pan_no,
        "corrected_address": corrected_address,
        "corrected_pincode": corrected_pincode,
        "corrected_value": corrected_value,
    }

    try:
        if property_key_clean not in ledger.property_index:
            raise ValueError(f"Property '{property_key_clean}' not found.")

        try:
            tx_record = ledger.get_property_transaction_by_id(property_key_clean, tx_id_clean)
        except ValueError:
            # If user entered an old/invalid id, fall back to latest VALID transaction.
            tx_id_clean = _latest_valid_transaction_id(ledger, property_key_clean)
            form_data["original_transaction_id"] = tx_id_clean
            tx_record = ledger.get_property_transaction_by_id(property_key_clean, tx_id_clean)

        if tx_record.get("status") != ledger.STATUS_VALID:
            tx_id_clean = _latest_valid_transaction_id(ledger, property_key_clean)
            form_data["original_transaction_id"] = tx_id_clean
            tx_record = ledger.get_property_transaction_by_id(property_key_clean, tx_id_clean)
            if tx_record.get("status") != ledger.STATUS_VALID:
                raise ValueError("No VALID transaction is currently available for this property.")

        requested_corrections = _build_requested_corrections(
            ledger,
            selected_fields=selected_fields_normalized,
            corrected_owner_name=corrected_owner_name,
            corrected_aadhar_no=corrected_aadhar_no,
            corrected_pan_no=corrected_pan_no,
            corrected_address=corrected_address,
            corrected_pincode=corrected_pincode,
            corrected_value=corrected_value,
        )

        request_id = _new_correction_request_id()
        correction = CorrectionRequest(
            request_id=request_id,
            property_key=property_key_clean,
            original_transaction_id=tx_id_clean,
            error_description=error_description.strip(),
            corrected_data_json=json.dumps(requested_corrections),
            supporting_notes=supporting_notes.strip(),
            submitted_officer_id=user.id,
            submitted_officer_name=user.username,
            status="PENDING_ADMIN_REVIEW",
        )
        db.add(correction)
        _append_correction_audit(
            db,
            correction_request_id=request_id,
            actor=user,
            action_type="SUBMITTED",
            comments=supporting_notes.strip(),
        )
        db.commit()

        _flash(request, f"Correction request {request_id} submitted for admin review.")
        return RedirectResponse(f"/corrections/{request_id}", status_code=302)
    except ValueError as exc:
        history = []
        state = None
        if property_key_clean in ledger.property_index:
            history = ledger.get_property_history(property_key_clean)
            state = ledger.get_property_current_state(property_key_clean)
        return templates.TemplateResponse(
            "correction_new.html",
            _ctx(
                request,
                db,
                flash={"message": str(exc), "category": "error"},
                form_data=form_data,
                property_history=history,
                current_state=state,
            ),
            status_code=200,
        )


@app.get("/corrections/{request_id}", response_class=HTMLResponse)
async def correction_detail(
    request_id: str,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    correction = db.query(CorrectionRequest).filter(CorrectionRequest.request_id == request_id).first()
    if correction is None:
        raise HTTPException(status_code=404, detail="Correction request not found.")

    if user.role == "officer" and correction.submitted_officer_id != user.id:
        raise HTTPException(status_code=403, detail="You can only view your own correction requests.")

    audit_logs = (
        db.query(CorrectionAuditLog)
        .filter(CorrectionAuditLog.correction_request_id == request_id)
        .order_by(CorrectionAuditLog.created_at.asc())
        .all()
    )
    ledger = _ledger(request)
    property_state = None
    if correction.property_key in ledger.property_index:
        property_state = ledger.get_property_current_state(correction.property_key)
    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "correction_detail.html",
        _ctx(
            request,
            db,
            correction=_serialize_correction_request(correction),
            property_state=property_state,
            audit_logs=audit_logs,
            flash=flash,
        ),
    )


@app.post("/corrections/{request_id}/admin-approve")
async def correction_admin_approve(
    request_id: str,
    request: Request,
    owner_name: str = Form(...),
    aadhar_no: str = Form(...),
    pan_no: str = Form(...),
    address: str = Form(""),
    pincode: str = Form(""),
    value: str = Form(...),
    comment: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    correction = db.query(CorrectionRequest).filter(CorrectionRequest.request_id == request_id).first()
    if correction is None:
        raise HTTPException(status_code=404, detail="Correction request not found.")

    if correction.status != "PENDING_ADMIN_REVIEW":
        _flash(request, "Only pending admin review requests can be approved.", "error")
        return RedirectResponse(f"/corrections/{request_id}", status_code=302)

    ledger = _ledger(request)
    try:
        corrected_payload = _normalize_correction_payload(
            ledger,
            owner_name=owner_name,
            aadhar_no=aadhar_no,
            pan_no=pan_no,
            address=address,
            pincode=pincode,
            value=value,
        )
        ledger.create_correction_transaction(
            property_key=correction.property_key,
            original_transaction_id=correction.original_transaction_id,
            corrected_data=corrected_payload,
            correction_request_id=correction.request_id,
            approved_by_authority=f"admin:{user.username}",
        )
        _record_block_activity(db, user, ledger.get_latest_block(), "APPLY_CORRECTION")
        db.commit()
        ledger._save_blockchain()
    except ValueError as exc:
        _flash(request, str(exc), "error")
        return RedirectResponse(f"/corrections/{request_id}", status_code=302)

    correction.corrected_data_json = json.dumps(corrected_payload)
    correction.status = "APPROVED"
    correction.finalized_at = datetime.now()
    _append_correction_audit(
        db,
        correction_request_id=request_id,
        actor=user,
        action_type="ADMIN_APPROVED_APPLIED",
        comments=comment.strip(),
    )
    db.commit()
    _flash(request, f"Request {request_id} approved and correction block added.")
    return RedirectResponse(f"/properties/{correction.property_key}", status_code=302)


@app.post("/corrections/{request_id}/admin-reject")
async def correction_admin_reject(
    request_id: str,
    request: Request,
    comment: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    correction = db.query(CorrectionRequest).filter(CorrectionRequest.request_id == request_id).first()
    if correction is None:
        raise HTTPException(status_code=404, detail="Correction request not found.")

    if correction.status != "PENDING_ADMIN_REVIEW":
        _flash(request, "Only pending admin review requests can be rejected.", "error")
        return RedirectResponse(f"/corrections/{request_id}", status_code=302)

    correction.status = "REJECTED"
    correction.finalized_at = datetime.now()
    _append_correction_audit(
        db,
        correction_request_id=request_id,
        actor=user,
        action_type="ADMIN_REJECTED",
        comments=comment.strip(),
    )
    db.commit()
    _flash(request, f"Request {request_id} was rejected.", "warning")
    return RedirectResponse(f"/corrections/{request_id}", status_code=302)


@app.post("/corrections/{request_id}/admin-request-changes")
async def correction_admin_request_changes(
    request_id: str,
    request: Request,
    comment: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    correction = db.query(CorrectionRequest).filter(CorrectionRequest.request_id == request_id).first()
    if correction is None:
        raise HTTPException(status_code=404, detail="Correction request not found.")

    if correction.status != "PENDING_ADMIN_REVIEW":
        _flash(request, "Only pending admin review requests can be sent back for changes.", "error")
        return RedirectResponse(f"/corrections/{request_id}", status_code=302)

    review_note = comment.strip()
    if not review_note:
        _flash(request, "Please provide feedback before requesting changes.", "error")
        return RedirectResponse(f"/corrections/{request_id}", status_code=302)

    _append_correction_audit(
        db,
        correction_request_id=request_id,
        actor=user,
        action_type="ADMIN_REQUESTED_CHANGES",
        comments=review_note,
    )
    db.commit()
    _flash(request, f"Requested changes for {request_id}. Officer has been notified in audit trail.", "warning")
    return RedirectResponse(f"/corrections/{request_id}", status_code=302)




# ---------------------------------------------------------------------------
# Admin — blockchain
# ---------------------------------------------------------------------------

@app.get("/validate", response_class=HTMLResponse)
async def validate_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    flash = _pop_flash(request)
    return templates.TemplateResponse("validate.html", _ctx(request, db, flash=flash))


@app.get("/blockchain", response_class=HTMLResponse)
async def blockchain_explorer(
    request: Request,
    page: int = 1,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    ledger = _ledger(request)
    page_size = 20
    total_blocks = len(ledger.chain)
    total_pages = max(1, (total_blocks + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    # Reverse order: newest first
    blocks = [b.to_dict() for b in reversed(ledger.chain)][start:end]

    return templates.TemplateResponse(
        "blockchain.html",
        _ctx(
            request, db,
            blocks=blocks,
            total_blocks=total_blocks,
            page=page,
            total_pages=total_pages,
            page_size=page_size,
        ),
    )


# ---------------------------------------------------------------------------
# Admin — user management
# ---------------------------------------------------------------------------

@app.get("/users", response_class=HTMLResponse)
async def users_overview(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)
    staff_users = db.query(User).order_by(User.created_at.asc()).all()
    staff_work_summary = _activity_summary_by_user(db)
    citizen_rows = _build_citizen_user_rows(db, ledger)
    flash = _pop_flash(request)

    return templates.TemplateResponse(
        "users_overview.html",
        _ctx(
            request,
            db,
            staff_users=staff_users,
            staff_work_summary=staff_work_summary,
            citizen_rows=citizen_rows,
            flash=flash,
        ),
    )


@app.get("/users/citizen/{citizen_ref}", response_class=HTMLResponse)
async def users_citizen_detail(
    citizen_ref: str,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)

    citizen = None
    ref = citizen_ref.strip()
    if ref.isdigit():
        citizen = db.query(Citizen).filter(Citizen.id == int(ref)).first()
    if citizen is None and ref:
        citizen = db.query(Citizen).filter(Citizen.customer_key == ref.upper()).first()

    if citizen is None:
        staff_users = db.query(User).order_by(User.created_at.asc()).all()
        staff_work_summary = _activity_summary_by_user(db)
        citizen_rows = _build_citizen_user_rows(db, ledger)
        return templates.TemplateResponse(
            "users_overview.html",
            _ctx(
                request,
                db,
                staff_users=staff_users,
                staff_work_summary=staff_work_summary,
                citizen_rows=citizen_rows,
                flash={"message": f"Citizen '{citizen_ref}' was not found.", "category": "error"},
            ),
            status_code=200,
        )

    props = ledger.get_properties_by_customer_key(
        citizen.customer_key,
        aadhar_no=citizen.aadhar_no,
        pan_no=citizen.pan_no,
        owner_name=citizen.name,
    )
    flash = _pop_flash(request)

    return templates.TemplateResponse(
        "user_citizen_detail.html",
        _ctx(
            request,
            db,
            citizen_user=citizen,
            current_properties=props.get("current", []),
            past_properties=props.get("past", []),
            flash=flash,
        ),
    )

@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    all_users = db.query(User).order_by(User.created_at).all()
    work_summary = _activity_summary_by_user(db)
    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "admin/users.html", _ctx(request, db, users=all_users, work_summary=work_summary, flash=flash)
    )


@app.get("/admin/users/create", response_class=HTMLResponse)
async def admin_create_user_page(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "admin/create_user.html", _ctx(request, db, flash=flash, form_data={})
    )


@app.post("/admin/users/create")
async def admin_create_user_post(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form("officer"),
):
    if role not in ("admin", "officer"):
        _flash(request, "Invalid role.", "error")
        return RedirectResponse("/admin/users/create", status_code=302)

    existing = db.query(User).filter(User.username == username).first()
    if existing:
        return templates.TemplateResponse(
            "admin/create_user.html",
            _ctx(
                request, db,
                flash={"message": f"Username '{username}' already exists.", "category": "error"},
                form_data={"username": username, "role": role},
            ),
            status_code=422,
        )

    new_user = User(
        username=username,
        hashed_password=hash_password(password),
        role=role,
        is_active=True,
    )
    db.add(new_user)
    db.commit()
    _flash(request, f"User '{username}' ({role}) created successfully.")
    return RedirectResponse("/admin/users", status_code=302)


@app.get("/admin/users/{user_id}/activity", response_class=HTMLResponse)
async def admin_user_activity(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    target = db.query(User).filter(User.id == user_id).first()
    if target is None:
        raise HTTPException(status_code=404, detail="User not found.")

    activities = (
        db.query(UserBlockActivity)
        .filter(UserBlockActivity.user_id == user_id)
        .order_by(UserBlockActivity.created_at.desc(), UserBlockActivity.id.desc())
        .all()
    )

    return templates.TemplateResponse(
        "admin/user_activity.html",
        _ctx(
            request,
            db,
            target_user=target,
            activities=activities,
            total_blocks=len(activities),
        ),
    )


@app.post("/admin/users/{user_id}/suspend")
async def admin_suspend_user(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    if user_id == current_user.id:
        _flash(request, "Cannot suspend your own account.", "error")
        return RedirectResponse("/admin/users", status_code=302)

    target = db.query(User).filter(User.id == user_id).first()
    if target:
        target.is_active = False
        db.commit()
        _flash(request, f"User '{target.username}' suspended.")
    return RedirectResponse("/admin/users", status_code=302)


@app.post("/admin/users/{user_id}/activate")
async def admin_activate_user(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    target = db.query(User).filter(User.id == user_id).first()
    if target:
        target.is_active = True
        db.commit()
        _flash(request, f"User '{target.username}' reactivated.")
    return RedirectResponse("/admin/users", status_code=302)


# ---------------------------------------------------------------------------
# HTMX partial endpoints
# ---------------------------------------------------------------------------

@app.get("/htmx/lookup-custid", response_class=HTMLResponse)
async def htmx_lookup_custid(
    request: Request,
    customer_key: str = "",
    target: str = "transfer",
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    """Look up owner info by customer key and return a partial to auto-fill form fields."""
    ledger = _ledger(request)
    cust_key = customer_key.strip().upper()

    if not cust_key:
        return HTMLResponse("")

    owner_info = ledger.get_owner_by_customer_key(cust_key)

    # Fallback: check the citizens table in SQLite
    if owner_info is None:
        citizen_record = db.query(Citizen).filter(Citizen.customer_key == cust_key).first()
        if citizen_record:
            owner_info = {
                "name": citizen_record.name,
                "aadhar": citizen_record.aadhar_no,
                "pan": citizen_record.pan_no,
                "customer_key": citizen_record.customer_key,
            }

    # Determine target field IDs based on form type
    if target == "inherit":
        name_field = "heir_name"
        aadhar_field = "heir_aadhar"
        pan_field = "heir_pan"
    else:
        name_field = "new_owner_name"
        aadhar_field = "new_owner_aadhar"
        pan_field = "new_owner_pan"

    if owner_info is None:
        return templates.TemplateResponse(
            "partials/custid_lookup.html",
            {"request": request, "error": f"Customer ID '{cust_key}' not found.", "owner": None,
             "name_field": name_field, "aadhar_field": aadhar_field, "pan_field": pan_field},
        )

    return templates.TemplateResponse(
        "partials/custid_lookup.html",
        {"request": request, "error": None, "owner": owner_info,
         "name_field": name_field, "aadhar_field": aadhar_field, "pan_field": pan_field},
    )

@app.get("/htmx/stamp-duty", response_class=HTMLResponse)
async def htmx_stamp_duty(
    request: Request,
    value: float = 0,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)
    if value <= 0:
        return HTMLResponse("")

    duty = ledger.calculate_stamp_duty(value)
    reg_fee = value * 0.05
    rate_label = ledger.stamp_duty_rate(value)
    total = value + duty + reg_fee

    return templates.TemplateResponse(
        "partials/stamp_duty.html",
        {
            "request": request,
            "value": value,
            "duty": duty,
            "reg_fee": reg_fee,
            "rate_label": rate_label,
            "total": total,
        },
    )


@app.post("/htmx/validate-aadhar", response_class=HTMLResponse)
async def htmx_validate_aadhar(
    request: Request,
    aadhar_no: str = Form(""),
    owner_name: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)
    error = None

    if not ledger.validate_aadhar(aadhar_no):
        error = "Must be exactly 12 digits."

    return templates.TemplateResponse(
        "partials/field_feedback.html",
        {"request": request, "error": error, "value": aadhar_no},
    )


@app.post("/htmx/validate-pan", response_class=HTMLResponse)
async def htmx_validate_pan(
    request: Request,
    pan_no: str = Form(""),
    owner_name: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)
    error = None

    if not ledger.validate_pan(pan_no):
        error = "Must be in format ABCDE1234F."

    return templates.TemplateResponse(
        "partials/field_feedback.html",
        {"request": request, "error": error, "value": pan_no},
    )


@app.post("/htmx/validate-survey", response_class=HTMLResponse)
async def htmx_validate_survey(
    request: Request,
    survey_no: str = Form(""),
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)
    error = None

    if not survey_no.strip():
        error = "Survey number is required."
    else:
        try:
            ledger.validate_survey_uniqueness(survey_no)
        except ValueError as exc:
            error = str(exc)

    return templates.TemplateResponse(
        "partials/field_feedback.html",
        {"request": request, "error": error, "value": survey_no},
    )

@app.get("/htmx/search", response_class=HTMLResponse)
async def htmx_search(
    request: Request,
    q: str = "",
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    query = q.strip()
    if not query:
        return HTMLResponse(
            '<p class="search-placeholder text-muted">'
            "Start typing to search across all fields."
            "</p>"
        )

    ledger = _ledger(request)
    results = ledger.search_properties(query)

    return templates.TemplateResponse(
        "partials/properties_list.html",
        {
            "request": request,
            "properties": results,
            "empty_message": f"No properties found matching '{query}'.",
        },
    )


@app.get("/htmx/validate-chain", response_class=HTMLResponse)
async def htmx_validate_chain(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    ledger = _ledger(request)
    is_valid = ledger.is_chain_valid()
    block_count = len(ledger.chain)

    return templates.TemplateResponse(
        "partials/chain_status.html",
        {"request": request, "is_valid": is_valid, "block_count": block_count},
    )


@app.post("/htmx/save-blockchain", response_class=HTMLResponse)
async def htmx_save_blockchain(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    ledger = _ledger(request)
    success = ledger._save_blockchain()
    message = "Blockchain saved successfully." if success else "Error saving blockchain."
    category = "success" if success else "error"

    return templates.TemplateResponse(
        "partials/flash.html",
        {"request": request, "flash": {"message": message, "category": category}},
    )


@app.get("/htmx/property-history/{key}", response_class=HTMLResponse)
async def htmx_property_history(
    key: str,
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_officer_or_admin),
):
    ledger = _ledger(request)
    try:
        history = ledger.get_property_history(key)
    except ValueError:
        return HTMLResponse('<p class="error-text">Property not found.</p>')

    return templates.TemplateResponse(
        "partials/history_accordion.html",
        _ctx(request, db, history=history, property_key=key, is_admin=user.role == "admin"),
    )


# ---------------------------------------------------------------------------
# 403 handler
# ---------------------------------------------------------------------------

@app.exception_handler(403)
async def forbidden_handler(request: Request, exc: HTTPException):
    db = SessionLocal()
    try:
        ctx = _ctx(request, db, detail=exc.detail)
    finally:
        db.close()
    return templates.TemplateResponse("403.html", ctx, status_code=403)


@app.exception_handler(404)
async def not_found_handler(request: Request, exc: HTTPException):
    db = SessionLocal()
    try:
        ctx = _ctx(request, db, detail=exc.detail)
    finally:
        db.close()
    return templates.TemplateResponse("404.html", ctx, status_code=404)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=False)
