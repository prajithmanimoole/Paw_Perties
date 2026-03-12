"""Authentication utilities: password hashing, session management, role guards."""

from __future__ import annotations

from typing import Optional

import bcrypt
from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from database import get_db
from models import User, Citizen


# ---------------------------------------------------------------------------
# Sentinel exception — caught by the app-level handler to issue a redirect
# ---------------------------------------------------------------------------

class NotAuthenticatedException(Exception):
    """Raised inside dependencies when no valid session exists."""


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def hash_password(plain: str) -> str:
    """Return a bcrypt hash of *plain*."""
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    """Return True if *plain* matches *hashed*."""
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

def set_session_user(request: Request, user: User) -> None:
    """Write the user id into the signed session cookie."""
    request.session["user_id"] = user.id
    request.session.pop("citizen_id", None)  # clear any citizen session


def set_session_citizen(request: Request, citizen: Citizen) -> None:
    """Write the citizen id into the signed session cookie."""
    request.session["citizen_id"] = citizen.id
    request.session.pop("user_id", None)  # clear any staff session


def clear_session(request: Request) -> None:
    """Destroy the current session."""
    request.session.clear()


def get_current_user(request: Request, db: Session = Depends(get_db)) -> Optional[User]:
    """Read user from session; returns None if not logged in or account inactive."""
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    user: Optional[User] = db.query(User).filter(User.id == user_id).first()
    if user is None or not user.is_active:
        clear_session(request)
        return None
    return user


def get_current_citizen(request: Request, db: Session = Depends(get_db)) -> Optional[Citizen]:
    """Read citizen from session; returns None if not logged in or account inactive."""
    citizen_id = request.session.get("citizen_id")
    if not citizen_id:
        return None
    citizen: Optional[Citizen] = db.query(Citizen).filter(Citizen.id == citizen_id).first()
    if citizen is None or not citizen.is_active:
        clear_session(request)
        return None
    return citizen


# ---------------------------------------------------------------------------
# Route guards (FastAPI dependencies)
# ---------------------------------------------------------------------------

def require_auth(
    request: Request,
    db: Session = Depends(get_db),
) -> User:
    """Raise NotAuthenticatedException (→ redirected to /login) if no valid session."""
    user = get_current_user(request, db)
    if user is None:
        raise NotAuthenticatedException()
    return user


def require_admin(user: User = Depends(require_auth)) -> User:
    """Ensure the authenticated user holds the *admin* role."""
    if user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required.",
        )
    return user


def require_officer(user: User = Depends(require_auth)) -> User:
    """Ensure the authenticated user holds the *officer* role only."""
    if user.role != "officer":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Officer access required.",
        )
    return user


def require_officer_or_admin(user: User = Depends(require_auth)) -> User:
    """Allow both officers and admins through."""
    return user  # require_auth already ensured authentication


def require_citizen(
    request: Request,
    db: Session = Depends(get_db),
) -> Citizen:
    """Ensure the current session belongs to a citizen. Redirects to /login otherwise."""
    citizen = get_current_citizen(request, db)
    if citizen is None:
        raise NotAuthenticatedException()
    return citizen
