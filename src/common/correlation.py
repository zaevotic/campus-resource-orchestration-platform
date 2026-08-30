import uuid
from contextvars import ContextVar
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

# Context variable for thread/task correlation ID
CORRELATION_ID_CTX: ContextVar[str] = ContextVar("correlation_id", default="")

CORRELATION_HEADER_NAME = "X-Correlation-ID"

def generate_correlation_id() -> str:
    return str(uuid.uuid4())

def get_current_correlation_id() -> str:
    cid = CORRELATION_ID_CTX.get()
    if not cid:
        cid = generate_correlation_id()
        CORRELATION_ID_CTX.set(cid)
    return cid

def set_current_correlation_id(cid: str) -> None:
    CORRELATION_ID_CTX.set(cid)

class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Extract from incoming header or generate new
        cid = request.headers.get(CORRELATION_HEADER_NAME) or generate_correlation_id()
        CORRELATION_ID_CTX.set(cid)
        
        response: Response = await call_next(request)
        response.headers[CORRELATION_HEADER_NAME] = cid
        return response
