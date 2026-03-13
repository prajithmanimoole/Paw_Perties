"""Pawperty — FastAPI application entry point.

Roles
-----
admin   : blockchain management + all property operations + user management
officer : property registration, transfer, inheritance, view, search
citizen : view own properties and ownership history (read-only)
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
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
from models import Citizen, User


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
    props = ledger.get_properties_by_customer_key(citizen.customer_key)
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
    props = ledger.get_properties_by_customer_key(citizen.customer_key)
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

    return templates.TemplateResponse(
        "index.html",
        _ctx(
            request,
            db,
            block_count=len(ledger.chain),
            property_count=len(all_props),
            recent_properties=recent,
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

@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users(
    request: Request,
    db: Session = Depends(get_db),
    user: User = Depends(require_admin),
):
    all_users = db.query(User).order_by(User.created_at).all()
    flash = _pop_flash(request)
    return templates.TemplateResponse(
        "admin/users.html", _ctx(request, db, users=all_users, flash=flash)
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

    # Fallback: check the citizens table in PostgreSQL
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
    else:
        try:
            ledger.validate_aadhar_uniqueness(owner_name.strip().upper(), aadhar_no)
        except ValueError as exc:
            error = str(exc)

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
    else:
        try:
            ledger.validate_pan_uniqueness(owner_name.strip().upper(), pan_no)
        except ValueError as exc:
            error = str(exc)

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
        {"request": request, "history": history, "property_key": key},
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
