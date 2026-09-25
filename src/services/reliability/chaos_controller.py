from fastapi import APIRouter, Depends
from src.services.auth_gateway.gateway import require_roles

chaos_router = APIRouter(prefix="/api/chaos", tags=["Chaos Engineering"])

# Global state for chaos injection
CHAOS_FLAGS = {
    "fail_notifications": False,
    "fail_analytics": False
}

@chaos_router.post("/inject")
def inject_failure(target: str, fail: bool, user=Depends(require_roles(["system_admin"]))):
    if target in CHAOS_FLAGS:
        CHAOS_FLAGS[target] = fail
        return {"status": f"Chaos flag '{target}' set to {fail}"}
    return {"error": "Invalid target"}