"""Auth router: register, login, get current user."""
from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError
from pydantic import BaseModel, EmailStr
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.models.models import User, AuditLog
from app.services.auth import (
    authenticate_user,
    create_access_token,
    decode_token,
    get_user_by_username,
    hash_password,
)
from app.config import get_settings

settings = get_settings()
router = APIRouter(prefix="/auth", tags=["Authentication"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


# ---- Pydantic Schemas ----
class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str
    role: str = "viewer"


class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user: dict


class UserOut(BaseModel):
    id: int
    username: str
    email: str
    role: str

    class Config:
        from_attributes = True


# ---- Dependency ----
async def get_current_user(
    token: Annotated[str, Depends(oauth2_scheme)],
    db: Session = Depends(get_db),
) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = decode_token(token)
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = get_user_by_username(db, username)
    if user is None:
        raise credentials_exception
    return user


def require_role(*roles: str):
    async def checker(current_user: User = Depends(get_current_user)):
        if current_user.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires one of roles: {roles}",
            )
        return current_user
    return checker


# ---- Endpoints ----
@router.post("/register", response_model=UserOut, status_code=201)
def register(req: RegisterRequest, db: Session = Depends(get_db)):
    if db.query(User).filter(User.username == req.username).first():
        raise HTTPException(status_code=400, detail="Username already taken")
    if db.query(User).filter(User.email == req.email).first():
        raise HTTPException(status_code=400, detail="Email already registered")

    user = User(
        username=req.username,
        email=req.email,
        hashed_password=hash_password(req.password),
        role=req.role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    log = AuditLog(user_id=user.id, action="REGISTER", table_name="users",
                   record_id=str(user.id), details={"username": user.username})
    db.add(log)
    db.commit()
    return user


@router.post("/login", response_model=TokenResponse)
def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Incorrect username or password")

    token = create_access_token(
        data={"sub": user.username},
        expires_delta=timedelta(minutes=settings.access_token_expire_minutes),
    )

    log = AuditLog(user_id=user.id, action="LOGIN", table_name="users",
                   record_id=str(user.id))
    db.add(log)
    db.commit()

    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {"id": user.id, "username": user.username, "email": user.email, "role": user.role},
    }


@router.get("/me", response_model=UserOut)
def get_me(current_user: User = Depends(get_current_user)):
    return current_user


@router.get("/users", dependencies=[Depends(require_role("admin"))])
def list_users(db: Session = Depends(get_db)):
    """Admin-only: list all users."""
    users = db.query(User).all()
    return [{"id": u.id, "username": u.username, "email": u.email, "role": u.role} for u in users]
