import asyncio
import time
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException, Depends, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.common.config import settings
from src.common.correlation import CorrelationIdMiddleware, get_current_correlation_id, CORRELATION_HEADER_NAME
from src.services.auth_gateway.auth_service import auth_service, User
from src.services.auth_gateway.gateway import get_current_user, require_roles, rate_limiter
from src.services.booking_service.resource_service import resource_service
from src.services.booking_service.availability import availability_service
from src.services.booking_service.booking import booking_service
from src.services.booking_service.service_request import service_request_mgr
from src.services.booking_service.outbox_relay import outbox_relay
from src.services.notification_service.consumer import notification_consumer
from src.services.analytics_service.consumer import analytics_consumer
from src.services.analytics_service.audit_log import audit_logger
from src.services.analytics_service.aggregator import analytics_aggregator
from src.services.reliability.dlq_manager import dlq_manager
from src.services.monitoring.metrics import metrics_collector
from src.common.message_broker import broker

# New Orchestration Components
from src.services.reliability.chaos_controller import chaos_router
from src.services.booking_service.waitlist_consumer import waitlist_consumer
from src.services.service_request.maintenance_saga import maintenance_saga

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: subscribe consumers and launch outbox relay worker
    notification_consumer.start()
    analytics_consumer.start()
    waitlist_consumer.start()
    maintenance_saga.start()
    await broker.start()   # real Kafka: connect producer + start one consumer loop per (topic, group)
    relay_task = asyncio.create_task(outbox_relay.run_loop())
    yield
    # Shutdown
    outbox_relay.stop()
    relay_task.cancel()
    await broker.stop()

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Campus Resource Orchestration Platform API",
    version="2.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Middleware: Correlation ID propagation (ANA-06)
app.add_middleware(CorrelationIdMiddleware)

# Request Metrics Middleware (NFR 1 & NFR 6)
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    client_ip = request.client.host if request.client else "unknown"
    if not rate_limiter.is_allowed(client_ip):
        return JSONResponse(status_code=429, content={"detail": "Too many requests. Rate limit exceeded."})
    
    start_time = time.perf_counter()
    try:
        response = await call_next(request)
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        metrics_collector.record_request(latency_ms, response.status_code)
        return response
    except Exception as e:
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        metrics_collector.record_request(latency_ms, 500)
        raise e

# Mount static folder
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.get("/")
def read_root():
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"message": "Campus Resource Orchestration Platform API is online", "version": "2.0.0"}

# Register Chaos Controller
app.include_router(chaos_router)

# ================= AUTHENTICATION & RBAC (M1-AUTH) =================
class LoginRequest(BaseModel):
    username: str
    password: str

class RegisterRequest(BaseModel):
    username: str
    password: str
    role: str
    department: str
    email: str

@app.post("/api/auth/login")
def login(req: LoginRequest):
    user = auth_service.authenticate(req.username, req.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    token = auth_service.create_jwt_token(user)
    audit_logger.log_action("USER_LOGIN", user.id, user.role, status="SUCCESS", details={"username": user.username})
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "username": user.username,
            "role": user.role,
            "department": user.department,
            "email": user.email
        }
    }

@app.post("/api/auth/register")
def register(req: RegisterRequest):
    try:
        user = auth_service.register_user(req.username, req.password, req.role, req.department, req.email)
        token = auth_service.create_jwt_token(user)
        audit_logger.log_action("USER_REGISTERED", user.id, user.role, status="SUCCESS", details={"username": user.username})
        return {"access_token": token, "user": user}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/auth/me")
def get_me(current_user: dict = Depends(get_current_user)):
    if not current_user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return current_user

# ================= RESOURCE MANAGEMENT & AVAILABILITY (M2-BOOKING / FR 2) =================
@app.get("/api/resources")
def list_resources(
    resource_type: Optional[str] = None,
    building: Optional[str] = None,
    min_capacity: Optional[int] = None
):
    return resource_service.list_resources(resource_type, building, min_capacity)

@app.get("/api/resources/{resource_id}")
def get_resource(resource_id: str):
    res = resource_service.get_resource(resource_id)
    if not res:
        raise HTTPException(status_code=404, detail="Resource not found")
    return res

class CreateResourceRequest(BaseModel):
    name: str
    resource_type: str
    building: str
    room_number: str
    capacity: int
    attributes: Optional[Dict[str, Any]] = None

@app.post("/api/resources")
def create_resource(
    req: CreateResourceRequest,
    user: dict = Depends(require_roles(["facility_admin", "system_admin"]))
):
    new_res = resource_service.create_resource(
        name=req.name,
        resource_type=req.resource_type,
        building=req.building,
        room_number=req.room_number,
        capacity=req.capacity,
        attributes=req.attributes or {}
    )
    audit_logger.log_action("CREATE_RESOURCE", user["sub"], user["role"], resource_id=new_res["id"], details=req.model_dump())
    return new_res

@app.get("/api/availability", dependencies=[Depends(get_current_user)])
def check_availability(
    resource_id: str = Query(...),
    start_time: str = Query(...),
    end_time: str = Query(...)
):
    return availability_service.check_availability(resource_id, start_time, end_time)

@app.get("/api/availability/{resource_id}/schedule", dependencies=[Depends(get_current_user)])
def get_resource_schedule(resource_id: str, date: str = Query(..., description="YYYY-MM-DD")):
    return availability_service.get_booked_intervals(resource_id, date)

# ================= BOOKINGS & CONFLICT PREVENTION (M2-BOOKING / FR 3 / STR-03) =================
class CreateBookingRequest(BaseModel):
    resource_id: str
    start_time: str
    end_time: str
    purpose: str
    attendees_count: int = 1

@app.post("/api/bookings")
async def create_booking(
    req: CreateBookingRequest,
    user: dict = Depends(get_current_user)
):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required to book a resource")
    
    cid = get_current_correlation_id()
    try:
        event = booking_service.create_booking(
            resource_id=req.resource_id,
            user_id=user["sub"],
            user_role=user["role"],
            department=user.get("department", "General"),
            start_time=req.start_time,
            end_time=req.end_time,
            purpose=req.purpose,
            attendees_count=req.attendees_count,
            correlation_id=cid
        )
        audit_logger.log_action(
            "CREATE_BOOKING", user["sub"], user["role"],
            resource_id=req.resource_id, status="SUCCESS",
            details={"booking_id": event["booking_id"], "purpose": req.purpose},
            correlation_id=cid
        )
        # Trigger outbox relay flush so the event moves to Kafka immediately
        await outbox_relay.process_pending_outbox()
        return event
    except ValueError as e:
        err_msg = str(e)
        status_code = 409 if "Double-booking conflict" in err_msg else 400
        audit_logger.log_action(
            "BOOKING_CONFLICT_REJECTED" if status_code == 409 else "BOOKING_VALIDATION_REJECTED",
            user["sub"], user["role"],
            resource_id=req.resource_id, status="REJECTED",
            details={"reason": err_msg}, correlation_id=cid
        )
        raise HTTPException(status_code=status_code, detail=err_msg)

@app.get("/api/bookings")
def list_bookings(
    resource_id: Optional[str] = None,
    status: Optional[str] = None,
    user: dict = Depends(get_current_user)
):
    user_filter = None
    if user and user["role"] not in ("system_admin", "facility_admin"):
        user_filter = user["sub"]
    return booking_service.list_bookings(user_id=user_filter, resource_id=resource_id, status=status)

class CancelBookingRequest(BaseModel):
    reason: Optional[str] = "User requested cancellation"

@app.post("/api/bookings/{booking_id}/cancel")
async def cancel_booking(
    booking_id: str,
    req: CancelBookingRequest = CancelBookingRequest(),
    user: dict = Depends(get_current_user)
):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    cid = get_current_correlation_id()
    try:
        event = booking_service.cancel_booking(
            booking_id=booking_id,
            user_id=user["sub"],
            user_role=user["role"],
            reason=req.reason or "Cancelled",
            correlation_id=cid
        )
        audit_logger.log_action(
            "CANCEL_BOOKING", user["sub"], user["role"],
            resource_id=event["resource_id"], status="SUCCESS",
            details={"booking_id": booking_id, "reason": req.reason},
            correlation_id=cid
        )
        await outbox_relay.process_pending_outbox()
        return event
    except PermissionError as pe:
        raise HTTPException(status_code=403, detail=str(pe))
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))

# ================= SERVICE REQUESTS (FR 4) =================
class CreateServiceRequest(BaseModel):
    resource_id: str
    issue_type: str
    description: str
    priority: str = "MEDIUM"

@app.post("/api/service-requests")
async def create_service_request(
    req: CreateServiceRequest,
    user: dict = Depends(get_current_user)
):
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    cid = get_current_correlation_id()
    event = service_request_mgr.create_request(
        resource_id=req.resource_id,
        requester_id=user["sub"],
        issue_type=req.issue_type,
        description=req.description,
        priority=req.priority,
        correlation_id=cid
    )
    audit_logger.log_action(
        "CREATE_SERVICE_REQUEST", user["sub"], user["role"],
        resource_id=req.resource_id, status="SUCCESS",
        details={"ticket_id": event["ticket_id"], "issue_type": req.issue_type},
        correlation_id=cid
    )
    await outbox_relay.process_pending_outbox()
    return event

@app.get("/api/service-requests", dependencies=[Depends(get_current_user)])
def list_service_requests(resource_id: Optional[str] = None, status: Optional[str] = None):
    return service_request_mgr.list_requests(resource_id, status)

# ================= NOTIFICATIONS (STR-05) =================
@app.get("/api/notifications", dependencies=[Depends(get_current_user)])
def list_notifications(limit: int = 50):
    return notification_consumer.list_recent_notifications(limit)

# ================= RELIABILITY & DLQ (STR-07) =================
@app.get("/api/dlq")
def list_dlq_events(user: dict = Depends(require_roles(["system_admin"]))):
    return dlq_manager.list_dlq_events()

@app.post("/api/dlq/{dlq_id}/replay")
async def replay_dlq_event(
    dlq_id: str,
    user: dict = Depends(require_roles(["system_admin"]))
):
    success = await dlq_manager.replay_dlq_event(dlq_id)
    if not success:
        raise HTTPException(status_code=404, detail="DLQ event not found")
    audit_logger.log_action("REPLAY_DLQ_EVENT", user["sub"], user["role"], status="SUCCESS", details={"dlq_id": dlq_id})
    return {"message": f"DLQ event {dlq_id} successfully replayed to original topic"}

# ================= ANALYTICS & OLAP AGGREGATIONS (M4-ANALYTICS / ANA-04) =================
@app.get("/api/analytics/utilization", dependencies=[Depends(get_current_user)])
def get_analytics_utilization():
    return analytics_aggregator.get_resource_utilization()

@app.get("/api/analytics/peak-hours", dependencies=[Depends(get_current_user)])
def get_analytics_peak_hours():
    return analytics_aggregator.get_peak_hours_analysis()

@app.get("/api/analytics/departments", dependencies=[Depends(get_current_user)])
def get_analytics_departments():
    return analytics_aggregator.get_department_distribution()

@app.get("/api/analytics/cancellations", dependencies=[Depends(get_current_user)])
def get_analytics_cancellations():
    return analytics_aggregator.get_cancellation_rates()

@app.get("/api/analytics/service-requests", dependencies=[Depends(get_current_user)])
def get_analytics_service_requests():
    return analytics_aggregator.get_service_request_summary()

# ================= AUDIT LOGS (ANA-03) =================
@app.get("/api/analytics/audit-logs")
def get_audit_logs(
    correlation_id: Optional[str] = None,
    actor_id: Optional[str] = None,
    resource_id: Optional[str] = None,
    limit: int = 100,
    user: dict = Depends(require_roles(["facility_admin", "system_admin"]))
):
    return audit_logger.query_audit_logs(correlation_id, actor_id, resource_id, limit)

# ================= MONITORING & SYSTEM OBSERVABILITY (ANA-05 / NFR 1,2,6) =================
@app.get("/health")
def health():
    return metrics_collector.get_health_status()

@app.get("/api/metrics")
def get_metrics():
    return metrics_collector.get_health_status()

@app.post("/api/relay/flush", dependencies=[Depends(require_roles(["system_admin"]))])
async def flush_outbox():
    count = await outbox_relay.process_pending_outbox()
    return {"processed_outbox_events": count}