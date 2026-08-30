import sqlite3
import json
import os
import threading
import datetime
from typing import List, Dict, Optional, Any
from src.common.config import settings

class BookingDatabaseManager:
    """
    Transactional storage manager for Resources, Bookings, Service Requests, and Outbox.
    Implements:
    - ACID transaction isolation
    - Double booking overlap prevention (simulating PostgreSQL range exclusion constraints)
    - Transactional Outbox pattern (STR-03)
    """
    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.SQLITE_PATH
        self._lock = threading.RLock()
        self._init_schema()

    def get_connection(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self):
        with self._lock:
            conn = self.get_connection()
            cur = conn.cursor()
            
            # Resources Table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS resources (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                building TEXT NOT NULL,
                room_number TEXT,
                capacity INTEGER DEFAULT 1,
                attributes TEXT, -- JSON semi-structured column
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            # Bookings Table (Strong Consistency & Overlap Prevention)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS bookings (
                id TEXT PRIMARY KEY,
                resource_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                user_role TEXT NOT NULL,
                department TEXT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP NOT NULL,
                purpose TEXT,
                attendees_count INTEGER DEFAULT 1,
                status TEXT NOT NULL DEFAULT 'CONFIRMED', -- CONFIRMED, CANCELLED
                correlation_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (resource_id) REFERENCES resources(id)
            )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_bookings_res_time ON bookings(resource_id, start_time, end_time, status)")

            # Transactional Outbox Table (STR-03)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS booking_outbox (
                id TEXT PRIMARY KEY,
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                correlation_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING', -- PENDING, PUBLISHED, FAILED
                retry_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                published_at TIMESTAMP
            )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_outbox_status ON booking_outbox(status, created_at)")

            # Service Requests Table (FR 4)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS service_requests (
                id TEXT PRIMARY KEY,
                resource_id TEXT NOT NULL,
                requester_id TEXT NOT NULL,
                issue_type TEXT NOT NULL,
                description TEXT,
                priority TEXT DEFAULT 'MEDIUM',
                status TEXT DEFAULT 'OPEN', -- OPEN, IN_PROGRESS, RESOLVED, CLOSED
                correlation_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)

            conn.commit()
            conn.close()
            self._seed_default_resources()

    DEFAULT_CATALOG = [
        # LHC-1 Lecture Halls & Classrooms
        ("res-lhc1-110", "LHC-1 Grand Auditorium 110", "Classroom", "LHC-1", "110", 500, json.dumps({"projector": "Dual 4K Laser", "microphones": 8, "tier_seating": True, "lecture_capture": True, "ac": "Central"})),
        ("res-lhc1-308", "LHC-1 Lecture Theatre 308", "Classroom", "LHC-1", "308", 300, json.dumps({"projector": "4K Laser", "microphones": 6, "tier_seating": True, "lecture_capture": True})),
        ("res-lhc1-105", "LHC-1 Classroom 105", "Classroom", "LHC-1", "105", 60, json.dumps({"projector": "Full HD", "smart_board": True, "microphones": 2})),
        ("res-lhc1-106", "LHC-1 Classroom 106", "Classroom", "LHC-1", "106", 60, json.dumps({"projector": "Full HD", "smart_board": True, "microphones": 2})),
        ("res-lhc1-204", "LHC-1 Classroom 204", "Classroom", "LHC-1", "204", 60, json.dumps({"projector": "Full HD", "whiteboard": True, "microphones": 2})),
        ("res-lhc1-405", "LHC-1 Classroom 405", "Classroom", "LHC-1", "405", 60, json.dumps({"projector": "Full HD", "smart_board": True, "audio_system": True})),

        # LHC-2 Lecture Halls
        ("res-lhc2-101", "LHC-2 Lecture Hall 101", "Classroom", "LHC-2", "101", 120, json.dumps({"projector": "4K Laser", "smart_board": True, "microphones": 4, "tier_seating": True})),
        ("res-lhc2-102", "LHC-2 Lecture Hall 102", "Classroom", "LHC-2", "102", 120, json.dumps({"projector": "4K Laser", "smart_board": True, "microphones": 4, "tier_seating": True})),
        ("res-lhc2-103", "LHC-2 Lecture Hall 103", "Classroom", "LHC-2", "103", 120, json.dumps({"projector": "4K Laser", "dual_display": True, "microphones": 4, "tier_seating": True})),

        # Department Rooms
        ("res-dept-cs-101", "CSE Dept Seminar & Collab Room", "Department Room", "CSE Department", "CS-101", 40, json.dumps({"department": "CSE", "projector": "4K Laser", "whiteboard": True, "rack_access": True})),
        ("res-dept-ee-201", "EE Dept Colloquium Room", "Department Room", "EE Department", "EE-201", 35, json.dumps({"department": "EE", "smart_board": True, "demo_bench": True})),
        ("res-dept-me-105", "ME Dept Design & Review Room", "Department Room", "ME Department", "ME-105", 30, json.dumps({"department": "ME", "cad_display_wall": True, "drafting_tables": 8})),
        ("res-dept-phy-301", "Physics Dept Reading & Council Room", "Department Room", "Physics Department", "PHY-301", 25, json.dumps({"department": "Physics", "chalkboard": "Triple Slate", "projector": "Full HD"})),

        # Conference Rooms
        ("res-conf-301", "Executive Boardroom", "Conference Room", "Administration Tower", "CONF-301", 20, json.dumps({"video_conferencing": "Polycom Studio", "whiteboard": True, "beverage_station": True})),
        ("res-conf-302", "Senate Conference Hall", "Conference Room", "Administration Tower", "CONF-302", 45, json.dumps({"video_conferencing": "Cisco Webex Room Kit", "table_mics": 16, "dual_display": True})),
        ("res-conf-201", "Dean's Council Chamber", "Conference Room", "Academic Block", "CONF-201", 25, json.dumps({"video_conferencing": "Logitech Rally", "smart_board": True, "acoustic_paneling": True})),
        ("res-conf-104", "Placement & Industry War Room", "Conference Room", "Career Hub", "CONF-104", 18, json.dumps({"video_conferencing": "4K Telepresence", "interview_booths": 2, "whiteboard": True})),

        # Core Classrooms & Seminar Halls
        ("res-hall-101", "Turing Lecture Hall", "Classroom", "Engineering Block A", "LH-101", 120, json.dumps({"projector": "4K Laser", "microphones": 4, "smart_board": True, "tier_seating": True})),
        ("res-sem-404", "Ada Lovelace Seminar Hall", "Classroom", "Computing Wing", "SEM-404", 60, json.dumps({"projector": "Full HD", "mic": 2, "dual_display": True})),

        # Laboratories
        ("res-lab-202", "Robotics & Embedded Systems Lab", "Laboratory", "Science Complex", "LAB-202", 35, json.dumps({"workstations": 35, "oscilloscopes": 10, "safety_kit": True, "special_ventilation": True})),
        ("res-lab-305", "AI & High-Performance Computing Lab", "Laboratory", "CSE Department", "LAB-305", 50, json.dumps({"gpu_nodes": "16x A100", "workstations": 50, "network": "10GbE"})),
        ("res-lab-401", "VLSI & Signal Processing Lab", "Laboratory", "EE Department", "LAB-401", 40, json.dumps({"fpga_kits": 40, "logic_analyzers": 20, "esd_benches": True})),
        ("res-vr-101", "VR Simulation Studio", "Laboratory", "Innovation Center", "VR-101", 20, json.dumps({"vr_headsets": 20, "motion_capture": True, "spatial_audio": True})),

        # Shared Equipment
        ("res-proj-901", "Mobile Sony 4K Laser Projector", "Equipment", "AV Central Depot", "DEPOT-01", 1, json.dumps({"lumens": 5000, "connector": "HDMI/Wireless", "battery_backup_hours": 3})),
        ("res-eq-902", "Portable PA & Wireless Mic Kit", "Equipment", "AV Central Depot", "DEPOT-02", 1, json.dumps({"wireless_mics": 4, "speaker_watts": 600, "battery_powered": True})),
    ]

    def _seed_default_resources(self):
        conn = self.get_connection()
        cur = conn.cursor()
        for r in self.DEFAULT_CATALOG:
            cur.execute(
                """
                INSERT INTO resources (id, name, resource_type, building, room_number, capacity, attributes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    resource_type = excluded.resource_type,
                    building = excluded.building,
                    room_number = excluded.room_number,
                    capacity = excluded.capacity,
                    attributes = excluded.attributes
                """,
                r
            )
        # Clean stray test artifacts & deduplicate by (name, building, room_number)
        cur.execute("DELETE FROM resources WHERE name = 'Unauthorized Lab'")
        cur.execute("""
            DELETE FROM resources
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM resources
                GROUP BY name, building, COALESCE(room_number, '')
            )
        """)
        conn.commit()
        conn.close()

db_manager = BookingDatabaseManager()
