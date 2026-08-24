import hashlib
import time
from typing import Optional, Dict
import jwt
from pydantic import BaseModel
from src.common.config import settings

class User(BaseModel):
    id: str
    username: str
    password_hash: str
    role: str  # student, faculty, facility_admin, system_admin
    department: str
    email: str

# In-memory user directory seeded with default accounts for all roles
USER_DB: Dict[str, User] = {}

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()

def seed_default_users():
    default_users = [
        User(id="usr-stud-01", username="student1", password_hash=hash_password("student123"), role="student", department="Computer Science", email="student1@campus.edu"),
        User(id="usr-fac-01", username="faculty1", password_hash=hash_password("faculty123"), role="faculty", department="Electrical Engineering", email="faculty1@campus.edu"),
        User(id="usr-fadm-01", username="admin_facility", password_hash=hash_password("facility123"), role="facility_admin", department="Facilities", email="facility@campus.edu"),
        User(id="usr-sys-01", username="admin_sys", password_hash=hash_password("sysadmin123"), role="system_admin", department="IT Services", email="sysadmin@campus.edu"),
    ]
    for u in default_users:
        USER_DB[u.username] = u

seed_default_users()

class AuthService:
    @staticmethod
    def authenticate(username: str, password: str) -> Optional[User]:
        user = USER_DB.get(username)
        if not user:
            return None
        if user.password_hash != hash_password(password):
            return None
        return user

    @staticmethod
    def register_user(username: str, password: str, role: str, department: str, email: str) -> User:
        if username in USER_DB:
            raise ValueError(f"User '{username}' already exists")
            
        # Security Hardening: Prevent privilege escalation
        if role in ["system_admin", "facility_admin"]:
            raise ValueError("Administrative roles must be provisioned by IT, not self-registered.")

        user = User(
            id=f"usr-{int(time.time() * 1000)}",
            username=username,
            password_hash=hash_password(password),
            role=role,
            department=department,
            email=email
        )
        USER_DB[username] = user
        return user

    @staticmethod
    def create_jwt_token(user: User) -> str:
        payload = {
            "sub": user.id,
            "username": user.username,
            "role": user.role,
            "department": user.department,
            "exp": time.time() + (settings.JWT_EXPIRATION_MINUTES * 60)
        }
        return jwt.encode(payload, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)

    @staticmethod
    def verify_token(token: str) -> Optional[dict]:
        try:
            payload = jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
            return payload
        except Exception:
            return None

auth_service = AuthService()