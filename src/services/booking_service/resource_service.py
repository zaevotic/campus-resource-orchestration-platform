import json
import uuid
from typing import List, Optional, Dict, Any
from src.services.booking_service.db import db_manager

class ResourceService:
    @staticmethod
    def list_resources(resource_type: Optional[str] = None, building: Optional[str] = None, min_capacity: Optional[int] = None) -> List[Dict[str, Any]]:
        conn = db_manager.get_connection()
        cur = conn.cursor()
        query = "SELECT * FROM resources WHERE is_active = 1"
        params = []
        if resource_type:
            query += " AND resource_type = ?"
            params.append(resource_type)
        if building:
            query += " AND building = ?"
            params.append(building)
        if min_capacity is not None:
            query += " AND capacity >= ?"
            params.append(min_capacity)
        query += " ORDER BY name ASC"
        cur.execute(query, params)
        rows = cur.fetchall()
        result = []
        seen = set()
        for r in rows:
            d = dict(r)
            dedup_key = (d.get("name"), d.get("building"), d.get("room_number") or "")
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            if d.get("attributes"):
                try:
                    d["attributes"] = json.loads(d["attributes"])
                except Exception:
                    pass
            result.append(d)
        conn.close()
        return result

    @staticmethod
    def get_resource(resource_id: str) -> Optional[Dict[str, Any]]:
        conn = db_manager.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM resources WHERE id = ?", (resource_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        d = dict(row)
        if d.get("attributes"):
            try:
                d["attributes"] = json.loads(d["attributes"])
            except Exception:
                pass
        return d

    @staticmethod
    def create_resource(name: str, resource_type: str, building: str, room_number: str, capacity: int, attributes: dict) -> Dict[str, Any]:
        if capacity < 1:
            raise ValueError("Resource capacity must be at least 1")
        attr_json = json.dumps(attributes) if attributes else "{}"
        conn = db_manager.get_connection()
        cur = conn.cursor()
        # Prevent duplicate resources with the same name, building, and room_number
        cur.execute(
            "SELECT id FROM resources WHERE name = ? AND building = ? AND COALESCE(room_number, '') = COALESCE(?, '') AND is_active = 1 LIMIT 1",
            (name, building, room_number)
        )
        existing = cur.fetchone()
        if existing:
            resource_id = existing["id"]
            cur.execute(
                "UPDATE resources SET resource_type = ?, capacity = ?, attributes = ? WHERE id = ?",
                (resource_type, capacity, attr_json, resource_id)
            )
            conn.commit()
            conn.close()
            return ResourceService.get_resource(resource_id)

        resource_id = f"res-{uuid.uuid4().hex[:8]}"
        cur.execute(
            "INSERT INTO resources (id, name, resource_type, building, room_number, capacity, attributes) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (resource_id, name, resource_type, building, room_number, capacity, attr_json)
        )
        conn.commit()
        conn.close()
        return ResourceService.get_resource(resource_id)

    @staticmethod
    def update_resource(resource_id: str, name: Optional[str] = None, capacity: Optional[int] = None, attributes: Optional[dict] = None) -> Optional[Dict[str, Any]]:
        res = ResourceService.get_resource(resource_id)
        if not res:
            return None
        conn = db_manager.get_connection()
        cur = conn.cursor()
        updates = []
        params = []
        if name is not None:
            updates.append("name = ?")
            params.append(name)
        if capacity is not None:
            updates.append("capacity = ?")
            params.append(capacity)
        if attributes is not None:
            updates.append("attributes = ?")
            params.append(json.dumps(attributes))
        if updates:
            params.append(resource_id)
            cur.execute(f"UPDATE resources SET {', '.join(updates)} WHERE id = ?", params)
            conn.commit()
        conn.close()
        return ResourceService.get_resource(resource_id)

resource_service = ResourceService()
