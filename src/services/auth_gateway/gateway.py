import time
from typing import List, Optional
from fastapi import Request, HTTPException, Security, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from src.services.auth_gateway.auth_service import auth_service, User, USER_DB

security = HTTPBearer(auto_error=False)

class RateLimiter:
    def __init__(self, requests_per_minute: int = 300):
        self.rpm = requests_per_minute
        self.client_records = {}

    def is_allowed(self, client_id: str) -> bool:
        now = time.time()
        window_start = now - 60
        history = self.client_records.get(client_id, [])
        history = [t for t in history if t > window_start]
        if len(history) >= self.rpm:
            return False
        history.append(now)
        self.client_records[client_id] = history
        return True

rate_limiter = RateLimiter(requests_per_minute=500)

async def get_current_user(credentials: Optional[HTTPAuthorizationCredentials] = Security(security)) -> Optional[dict]:
    if not credentials:
        return None
    token = credentials.credentials
    payload = auth_service.verify_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired authentication token")
    return payload

def require_roles(allowed_roles: List[str]):
    def role_checker(current_user: Optional[dict] = Depends(get_current_user)):
        if not current_user:
            raise HTTPException(status_code=401, detail="Authentication required")
        user_role = current_user.get("role")
        if user_role not in allowed_roles and user_role != "system_admin":
            raise HTTPException(
                status_code=403, 
                detail=f"Access denied for role '{user_role}'. Required roles: {allowed_roles}"
            )
        return current_user
    return role_checker
