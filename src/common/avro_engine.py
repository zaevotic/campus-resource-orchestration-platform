import io
import json
from pathlib import Path
from typing import Dict, Any
import fastavro

SCHEMA_DIR = Path(__file__).resolve().parent.parent.parent / "schemas"

class AvroSchemaRegistry:
    def __init__(self):
        self._parsed_schemas: Dict[str, Any] = {}
        self._raw_schemas: Dict[str, dict] = {}
        self._load_schemas()

    def _load_schemas(self):
        schema_files = {
            "BookingConfirmed": SCHEMA_DIR / "booking_confirmed.avsc",
            "BookingCancelled": SCHEMA_DIR / "booking_cancelled.avsc",
            "ServiceRequestCreated": SCHEMA_DIR / "service_request.avsc",
            "AuditEvent": SCHEMA_DIR / "audit_event.avsc",
            "DeadLetterEvent": SCHEMA_DIR / "dlq_event.avsc",
        }
        for event_name, path in schema_files.items():
            if path.exists():
                with open(path, "r", encoding="utf-8") as f:
                    schema_dict = json.load(f)
                    self._raw_schemas[event_name] = schema_dict
                    self._parsed_schemas[event_name] = fastavro.parse_schema(schema_dict)

    def get_schema(self, event_type: str):
        return self._parsed_schemas.get(event_type)

    def validate(self, event_type: str, record: dict) -> bool:
        parsed = self.get_schema(event_type)
        if not parsed:
            return False
        return fastavro.validate(record, parsed)

    def serialize(self, event_type: str, record: dict) -> bytes:
        parsed = self.get_schema(event_type)
        if not parsed:
            # Fallback to UTF-8 JSON if schema not registered
            return json.dumps(record).encode("utf-8")
        try:
            out = io.BytesIO()
            fastavro.schemaless_writer(out, parsed, record)
            return out.getvalue()
        except Exception:
            return json.dumps(record).encode("utf-8")

    def deserialize(self, event_type: str, data: bytes) -> dict:
        parsed = self.get_schema(event_type)
        if not parsed:
            return json.loads(data.decode("utf-8"))
        try:
            inp = io.BytesIO(data)
            return fastavro.schemaless_reader(inp, parsed)
        except Exception:
            # Handle possible JSON fallback
            return json.loads(data.decode("utf-8"))

schema_registry = AvroSchemaRegistry()
