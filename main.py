from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Literal, Optional

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from google import genai
from groq import AsyncGroq
from pydantic import BaseModel, Field

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("eduagent")

# ---------------------------------------------------------------------------
# WHITE KNIGHTS CAMPUS DATABASE — 1,200 Students + Faculty Seed
# ---------------------------------------------------------------------------

def _seeded_random(seed: str) -> int:
    h = 0
    for ch in seed:
        h = ((h << 5) - h) + ord(ch)
        h &= 0xFFFFFFFF
        if h >= 0x80000000:
             h -= 0x100000000
    return abs(h)

def _seeded_pick(arr, seed: str, offset: int = 0):
    return arr[_seeded_random(seed + str(offset)) % len(arr)]

def _seeded_range(min_val: int, max_val: int, seed: str, offset: int = 0) -> int:
    return min_val + (_seeded_random(seed + str(offset)) % (max_val - min_val + 1))

def _init_white_knights_database() -> dict[str, dict[str, Any]]:
    database: dict[str, dict[str, Any]] = {}
    first_names  = ["Arun","Bharath","Sanjay","Rahul","Deepak","Vikram","Rohan","Karthik",
                    "Abhishek","Hari","Arjun","Suresh","Priya","Anjali","Sneha","Divya",
                    "Pooja","Meena","Ananya","Neha","Rohani","Kavitha","Lakshmi","Sindhu"]
    last_names   = ["Raj","Kumar","Sharma","Verma","Nair","Patel","Reddy","Murugan",
                    "Das","Pillai","Rao","Srinivasan","Iyer","Menon","Gounder","Joshi","Murthy","Swamy"]
    female_names = {"Priya","Anjali","Sneha","Divya","Pooja","Meena","Ananya","Neha",
                    "Rohani","Kavitha","Lakshmi","Sindhu"}
    clubs        = ["AI Research & Robotics Club","Fine Arts & Cultural Club",
                    "Google Developer Student Club (GDSC)","Eco-Green Environmental Club",
                    "Sports & Athletics League"]
    subjects_by_year = {
        1: ["Technical English","Engineering Maths-I","Engineering Physics","Problem Solving in C"],
        2: ["Data Structures","Discrete Mathematics","Digital Electronics","OOPs Using C++"],
        3: ["Python Programming","Operations Research","DBMS","Artificial Intelligence"],
        4: ["Cloud Computing","Cyber Security","Big Data Analytics","Major Project Phase-II"],
    }
    faculty_pool = ["Dr. R. Srinivasan","Prof. M. Anjali","Dr. K. Vikram",
                    "Prof. S. Rohan","Dr. P. Nair","Prof. A. Kumar"]
    days         = ["Monday","Tuesday","Wednesday","Thursday","Friday"]
    year_configs = [
        {"num":1,"label":"1st Year (Batch 2025-2029)","prefix":"25"},
        {"num":2,"label":"2nd Year (Batch 2024-2028)","prefix":"24"},
        {"num":3,"label":"3rd Year (Batch 2023-2027)","prefix":"23"},
        {"num":4,"label":"4th Year (Batch 2022-2026)","prefix":"22"},
    ]
    depts = ["CSE","ECE","MECH","IT"]

    for year in year_configs:
        subjects = subjects_by_year[year["num"]]
        for dept in depts:
            for i in range(1, 76):
                roll = f"{year['prefix']}WK{dept}{str(i).zfill(3)}"
                fn   = _seeded_pick(first_names, roll, 1)
                ln   = _seeded_pick(last_names,  roll, 2)
                mod  = i % 10
                std  = "elite" if mod == 0 else ("critical" if mod in (3, 7) else "average")

                attendance = {}; it = {}; um = {}; ss = {}
                for idx, sub in enumerate(subjects):
                    seed = roll + sub
                    if std == "elite":
                        a, s = _seeded_range(94,100,seed,10), _seeded_range(88,99,seed,11)
                    elif std == "critical":
                        a, s = _seeded_range(55,74,seed,12), _seeded_range(32,49,seed,13)
                    else:
                        a, s = _seeded_range(76,93,seed,14), _seeded_range(52,87,seed,15)

                    grade = ("O (Outstanding)" if s>=90 else "A+ (Excellent)" if s>=80
                             else "B (Pass)" if s>=50 else "RA (Re-Appearance Required)")
                    attendance[sub] = f"{a}%"
                    it[sub]  = s
                    um[sub]  = grade
                    ss[sub]  = faculty_pool[idx % len(faculty_pool)]

                tt = {}
                for day in days:
                    sh = sorted(subjects, key=lambda x: _seeded_random(roll+day+x))
                    tt[day] = f"{sh[0]}, {sh[1]}, {sh[2]}"

                fee_due  = (i % 3 == 0)
                tuition  = 120000
                paid     = _seeded_range(60000,90000,roll,20) if fee_due else tuition

                database[roll] = {
                    "role": "student",
                    "password": "password123",
                    "profile": {
                        "name": f"{fn} {ln}", "roll_number": roll,
                        "department": f"B.Tech {dept}", "batch_label": year["label"],
                        "academic_year": year["num"],
                        "gender": "female" if fn in female_names else "male",
                        "club_membership": _seeded_pick(clubs, roll, 3),
                    },
                    "finance": {
                        "status": "Balance Due" if fee_due else "Fully Paid",
                        "tuition_fee_total": tuition, "paid_amount": paid,
                        "balance_due": tuition - paid, "due_date": "July 30, 2026",
                    },
                    "academics": {
                        "attendance": attendance, "institution_test": it,
                        "university_marks": um, "staffs_and_subjects": ss, "timetable": tt,
                    },
                }

    for fid, fname in zip(
        ["DR_SRINIVASAN_WK","PROF_ANJALI_WK","DR_VIKRAM_WK","PROF_ROHAN_WK"],
        ["Dr. R. Srinivasan","Prof. M. Anjali","Dr. K. Vikram","Prof. S. Rohan"],
    ):
        database[fid] = {
            "role": "faculty", "password": "password123",
            "profile": {
                "name": fname, "staff_id": fid,
                "department": "Information Technology / CSE",
                "designation": "Senior Professor" if "Dr." in fname else "Assistant Professor",
            },
            "duties": {
                "weekly_target_hours": 5, "current_assigned_hours": 4,
                "pending_substitutions_slots": [],
            },
        }

    logger.info("White Knights ERP seeded: %d records.", len(database))
    return database

# ---------------------------------------------------------------------------
# DATA CLASSES & PYDANTIC MODELS
# ---------------------------------------------------------------------------

@dataclass
class BlastResult:
    source:     str
    text:       str
    elapsed_ms: float
    error:      Optional[str] = None

class AskEduAgentRequest(BaseModel):
    roll_number:   str = Field(..., min_length=1)
    subject_code:  str = Field(..., min_length=1)
    student_query: str = Field(..., min_length=1)

class AskEduAgentResponse(BaseModel):
    track:        Literal["personal", "academic"]
    roll_number:  str
    subject_code: str
    answer:       str
    latency_ms:   float
    meta:         dict[str, Any] = Field(default_factory=dict)

class StudentSyncRequest(BaseModel):
    roll_number: str

class UploadMaterialRequest(BaseModel):
    subject_code: str
    unit:         str
    heading:      str
    body_text:    str

class AttendanceTokenRequest(BaseModel):
    room_code:        str
    duration_seconds: int = 60

class MassNotificationRequest(BaseModel):
    target_student_rolls: list[str]
    announcement_message: str

class CreateRoomResponse(BaseModel):
    room_code: str

# ---------------------------------------------------------------------------
# WEBSOCKET ROOM MANAGER
# ---------------------------------------------------------------------------

@dataclass
class RoomMember:
    websocket: WebSocket
    roll:      str
    name:      str

class RoomManager:
    def __init__(self):
        self.rooms: dict[str, list[RoomMember]] = defaultdict(list)

    def create_room(self) -> str:
        code = str(uuid.uuid4())[:6].upper()
        self.rooms[code] = []
        return code

    def room_exists(self, code: str) -> bool:
        return code in self.rooms

    async def connect(self, code: str, member: RoomMember) -> None:
        self.rooms[code].append(member)
        await member.websocket.send_json({"type": "joined", "room": code})

    def disconnect(self, code: str, member: RoomMember) -> None:
        self.rooms[code] = [m for m in self.rooms[code] if m.websocket != member.websocket]
        if not self.rooms[code]:
            self.rooms.pop(code, None)

    async def broadcast(self, code: str, payload: dict, exclude: WebSocket | None = None) -> None:
        dead = []
        for m in self.rooms.get(code, []):
            if m.websocket == exclude:
                continue
            try:
                await m.websocket.send_json(payload)
            except Exception:
                dead.append(m)
        for m in dead:
            self.disconnect(code, m)

    async def broadcast_all(self, code: str, payload: dict) -> None:
        await self.broadcast(code, payload, exclude=None)

    def member_list(self, code: str) -> list[dict]:
        return [{"roll": m.roll, "name": m.name} for m in self.rooms.get(code, [])]

room_manager = RoomManager()

# ---------------------------------------------------------------------------
# FACULTY SESSION MANAGER
# ---------------------------------------------------------------------------

class FacultySessionManager:
    def __init__(self):
        self.sessions: dict[str, dict[str, Any]] = {}

    def start_attendance_session(self, room_code: str, duration_secs: int = 60) -> None:
        self.sessions[room_code] = {
            "expires_at": time.time() + duration_secs, "active": True
        }
        logger.info("Attendance window opened: %s (%ds)", room_code, duration_secs)

    def is_session_valid(self, room_code: str) -> bool:
        s = self.sessions.get(room_code)
        if not s:
            return True
        if not s["active"] or time.time() > s["expires_at"]:
            s["active"] = False
            return False
        return True

faculty_session_manager = FacultySessionManager()
active_campus_sockets: dict[str, WebSocket] = {}

# ---------------------------------------------------------------------------
# MODEL CONFIG
# ---------------------------------------------------------------------------

GEMINI_MODEL_RESEARCH = os.getenv("GEMINI_MODEL_RESEARCH", "gemini-2.0-flash")
GEMINI_MODEL_BLAST    = os.getenv("GEMINI_MODEL_BLAST",    "gemini-2.0-flash")
GEMINI_MODEL_JUDGE    = os.getenv("GEMINI_MODEL_JUDGE",    "gemini-2.0-flash")
GEMINI_MODEL_PARSER   = os.getenv("GEMINI_MODEL_PARSER",   "gemini-2.0-flash")
GROQ_MODEL_X          = os.getenv("GROQ_MODEL_X",          "llama-3.1-8b-instant")
GROQ_MODEL_Y          = os.getenv("GROQ_MODEL_Y",          "mixtral-8x7b-32768")

PERSONAL_KEYWORDS = re.compile(
    r"\b(attendance|mark|marks|score|grade|present|absent|fee|fees|timetable|schedule|work|today|class|classes|subject|subjects|balance|paid|due)\b",
    re.IGNORECASE,
)

LOW_ATT_WARNING_PCT = 78

WK_ERP_BASE  = os.getenv("WK_ERP_BASE",  "https://white-knights-erp.onrender.com")
WK_ERP_TOKEN = os.getenv("WK_ERP_TOKEN", "WK_DEV_TOKEN_2025")

# ---------------------------------------------------------------------------
# MONGODB LAYER
# ---------------------------------------------------------------------------

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/eduagent")
mongo_db  = None
IN_MEMORY_DB: dict[str, dict[str, Any]] = {}

FALLBACK_PRIVATE_HISTORY:  dict[str, list[dict]] = defaultdict(list)
FALLBACK_GROUP_HISTORY:    dict[str, list[dict]] = defaultdict(list)
FALLBACK_STUDY_MATERIALS:  list[dict]            = []
FALLBACK_SNAPSHOTS:        dict[str, dict]       = {}

try:
    import motor.motor_asyncio as motor_asyncio
    MOTOR_AVAILABLE = True
except ImportError:
    MOTOR_AVAILABLE = False
    logger.warning("motor not installed — in-memory only.")

async def _seed_mongo_if_empty(collection) -> None:
    count = await collection.count_documents({})
    if count == 0:
        logger.info("MongoDB empty — seeding %d records …", len(IN_MEMORY_DB))
        await collection.insert_many(list(IN_MEMORY_DB.values()))
        logger.info("Seeding complete.")
    else:
        logger.info("MongoDB already has %d records.", count)

async def db_get_user(token_id: str) -> dict[str, Any] | None:
    if mongo_db is not None:
        try:
            doc = await mongo_db["users"].find_one({"profile.roll_number": token_id}, {"_id": 0})
            if not doc:
                doc = await mongo_db["users"].find_one({"profile.staff_id": token_id}, {"_id": 0})
            if doc:
                return doc
        except Exception:
            pass
    
    headers = {"X-System-Token": WK_ERP_TOKEN}
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            if "FAC_" in token_id or "WK" in token_id:
                r = await client.get(f"{WK_ERP_BASE.rstrip('/')}/faculty/{token_id}", headers=headers)
                if r.status_code == 200:
                    data = r.json()
                    return {"role": "faculty", "profile": {"name": data["name"], "staff_id": token_id}}
            else:
                r = await client.get(f"{WK_ERP_BASE.rstrip('/')}/student/{token_id}", headers=headers)
                if r.status_code == 200:
                    data = r.json()
                    return {"role": "student", "profile": {"name": data["name"], "roll_number": token_id}}
    except Exception:
        pass
    return None

async def db_save_message(coll: str, roll: str, room: str, message: dict) -> None:
    if mongo_db is None:
        FALLBACK_PRIVATE_HISTORY[roll].append(message)
        return
    try:
        await mongo_db[coll].insert_one(
            {"roll_number": roll, "room": room, "message": message, "ts": time.time()})
    except Exception:
        FALLBACK_PRIVATE_HISTORY[roll].append(message)

async def db_load_history(coll: str, roll: str, room: str, limit: int = 50) -> list[dict]:
    if mongo_db is None:
        return FALLBACK_PRIVATE_HISTORY[roll][-limit:]
    try:
        cursor = mongo_db[coll].find(
            {"roll_number": roll, "room": room}, {"_id": 0, "message": 1}
        ).sort("ts", -1).limit(limit)
        docs = await cursor.to_list(length=limit)
        return [d["message"] for d in reversed(docs)]
    except Exception:
        return FALLBACK_PRIVATE_HISTORY[roll][-limit:]

async def db_load_room_history(room: str, limit: int = 100) -> list[dict]:
    if mongo_db is None:
        return FALLBACK_GROUP_HISTORY[room][-limit:]
    try:
        cursor = mongo_db["group_messages"].find(
            {"room": room}, {"_id": 0, "message": 1}
        ).sort("ts", -1).limit(limit)
        docs = await cursor.to_list(length=limit)
        return [d["message"] for d in reversed(docs)]
    except Exception:
        return FALLBACK_GROUP_HISTORY[room][-limit:]

async def db_save_group_message(room: str, message: dict) -> None:
    if mongo_db is None:
        FALLBACK_GROUP_HISTORY[room].append(message)
        return
    try:
        await mongo_db["group_messages"].insert_one(
            {"room": room, "message": message, "ts": time.time()})
    except Exception:
        FALLBACK_GROUP_HISTORY[room].append(message)

async def db_save_study_material(subject_code: str, unit: str, heading: str, body: str) -> None:
    doc = {"subject_code": subject_code, "unit": unit, "heading": heading,
           "body": body, "ts": time.time()}
    if mongo_db is None:
        FALLBACK_STUDY_MATERIALS.append(doc)
        return
    try:
        await mongo_db["study_materials"].insert_one(doc)
    except Exception:
        FALLBACK_STUDY_MATERIALS.append(doc)

async def db_load_study_materials(subject_code: str) -> list[dict]:
    if mongo_db is None:
        return [m for m in FALLBACK_STUDY_MATERIALS if m["subject_code"] == subject_code]
    try:
        cursor = mongo_db["study_materials"].find(
            {"subject_code": subject_code}, {"_id": 0}).sort("ts", -1)
        return await cursor.to_list(length=100)
    except Exception:
        return [m for m in FALLBACK_STUDY_MATERIALS if m["subject_code"] == subject_code]

async def db_save_snapshot(roll: str, snapshot: dict) -> None:
    doc = {"roll_number": roll, "snapshot": snapshot, "synced_at": time.time()}
    if mongo_db is None:
        FALLBACK_SNAPSHOTS[roll] = doc
        return
    try:
        await mongo_db["student_snapshots"].update_one(
            {"roll_number": roll}, {"$set": doc}, upsert=True)
    except Exception:
        FALLBACK_SNAPSHOTS[roll] = doc

async def db_get_snapshot(roll: str) -> dict | None:
    if mongo_db is None:
        return FALLBACK_SNAPSHOTS.get(roll)
    try:
        return await mongo_db["student_snapshots"].find_one(
            {"roll_number": roll}, {"_id": 0})
    except Exception:
        return FALLBACK_SNAPSHOTS.get(roll)

async def db_update_last_seen(roll: str) -> None:
    doc = {"roll_number": roll, "last_seen": time.time()}
    if mongo_db is None:
        FALLBACK_SNAPSHOTS.setdefault(roll, {})["last_seen_ts"] = time.time()
        return
    try:
        await mongo_db["last_seen"].update_one(
            {"roll_number": roll}, {"$set": doc}, upsert=True)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# ASYNC ERP COUPLING HELPER
# ---------------------------------------------------------------------------

async def fetch_from_erp(path: str) -> dict | None:
    url = f"{WK_ERP_BASE.rstrip('/')}{path}"
    headers = {"X-System-Token": WK_ERP_TOKEN}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code == 200:
                return resp.json()
            logger.warning("ERP service returned status %d for %s", resp.status_code, path)
    except Exception as exc:
        logger.error("Failed outbound connection to ERP server: %s", exc)
    return None

# ---------------------------------------------------------------------------
# 7-KEY CLIENT POOL
# ---------------------------------------------------------------------------

@dataclass
class ClientPool:
    gemini_judge_primary: genai.Client
    gemini_judge_backup:  genai.Client
    gemini_researcher:    genai.Client
    gemini_blaster:       genai.Client
    groq_x:               AsyncGroq
    groq_y:               AsyncGroq
    gemini_heavy_parser:  genai.Client

def _require_key(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def build_client_pool() -> ClientPool:
    return ClientPool(
        gemini_judge_primary=genai.Client(api_key=_require_key("GEMINI_KEY_A")),
        gemini_judge_backup =genai.Client(api_key=_require_key("GEMINI_KEY_A_BACKUP")),
        gemini_researcher   =genai.Client(api_key=_require_key("GEMINI_KEY_B")),
        gemini_blaster      =genai.Client(api_key=_require_key("GEMINI_KEY_C")),
        groq_x              =AsyncGroq(api_key=_require_key("GROQ_KEY_X")),
        groq_y              =AsyncGroq(api_key=_require_key("GROQ_KEY_Y")),
        gemini_heavy_parser =genai.Client(api_key=_require_key("GEMINI_KEY_FILE_PARSER")),
    )

clients: Optional[ClientPool] = None

# ---------------------------------------------------------------------------
# BRICK 2 — DAILY AUTO-SYNC ENGINE (runs every day at 16:30 local time)
# ---------------------------------------------------------------------------

def _detect_changes(old_snap: dict, new_data: dict) -> list[str]:
    changes: list[str] = []
    old_acc = old_snap.get("academics", {})
    new_acc = new_data.get("academics", {})

    old_att = old_acc.get("attendance", {})
    new_att = new_data.get("attendance", new_acc.get("attendance", {}))
    for sub, new_pct in new_att.items():
        old_str = old_att.get(sub, "0%")
        old_val = int(old_str.rstrip("%")) if isinstance(old_str, str) else int(old_str)
        new_val = int(new_pct.rstrip("%")) if isinstance(new_pct, str) else int(new_pct)
        if new_val != old_val:
            direction = "📉 dropped" if new_val < old_val else "📈 improved"
            changes.append(f"Attendance in {sub} {direction}: {old_val}% → {new_val}%")

    old_it = old_acc.get("institution_test", {})
    new_it = new_acc.get("institution_test", {})
    for sub, new_score in new_it.items():
        old_score = old_it.get(sub, 0)
        if new_score != old_score:
            changes.append(f"Marks updated in {sub}: {old_score} → {new_score}")

    return changes

async def _sync_one_student(roll: str) -> list[str]:
    fresh = await fetch_from_erp(f"/student/{roll}")
    if not fresh:
        fresh = IN_MEMORY_DB.get(roll)
    if not fresh or fresh.get("role") == "faculty":
        return []

    old_doc = await db_get_snapshot(roll)
    changes: list[str] = []
    if old_doc:
        changes = _detect_changes(old_doc.get("snapshot", {}), fresh)
    await db_save_snapshot(roll, fresh)
    return changes

async def _daily_sync_all_students() -> None:
    logger.info("🔄 Daily 4:30 PM auto-sync started (%d students).", len(IN_MEMORY_DB))
    total_changes = 0
    student_rolls = [k for k, v in IN_MEMORY_DB.items() if v.get("role") == "student"]
    for roll in student_rolls:
        changes = await _sync_one_student(roll)
        if changes:
            total_changes += len(changes)
            sock = active_campus_sockets.get(roll)
            if sock:
                try:
                    change_text = "\n".join(f"• {c}" for c in changes)
                    await sock.send_json({
                        "type":  "agent_reply",
                        "text":  f"🔔 ERP Update Detected:\n\n{change_text}",
                        "track": "personal",
                        "ts":    time.time(),
                    })
                    logger.info("Notified %s — %d change(s).", roll, len(changes))
                except Exception:
                    pass
    logger.info("✅ Daily sync complete. Total changes detected: %d.", total_changes)

async def _schedule_daily_sync() -> None:
    while True:
        now = datetime.now()
        target = now.replace(hour=16, minute=30, second=0, microsecond=0)
        if now >= target:
            target = target + timedelta(days=1)
        wait_secs = (target - now).total_seconds()
        logger.info("⏰ Next auto-sync scheduled in %.0f seconds (16:30).", wait_secs)
        await asyncio.sleep(wait_secs)
        await _daily_sync_all_students()

# ---------------------------------------------------------------------------
# LIFESPAN
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global clients, mongo_db, IN_MEMORY_DB
    IN_MEMORY_DB = _init_white_knights_database()
    if MOTOR_AVAILABLE:
        try:
            mc       = motor_asyncio.AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            mongo_db = mc["eduagent"]
            await _seed_mongo_if_empty(mongo_db["users"])
            await mongo_db["private_messages"].create_index([("roll_number",1),("room",1),("ts",-1)])
            await mongo_db["group_messages"].create_index([("room",1),("ts",-1)])
            await mongo_db["study_materials"].create_index([("subject_code",1),("ts",-1)])
            await mongo_db["student_snapshots"].create_index([("roll_number",1)], unique=True)
            await mongo_db["last_seen"].create_index([("roll_number",1)], unique=True)
            logger.info("MongoDB connected and indexes created.")
        except Exception as exc:
            logger.warning("MongoDB failed (%s) — in-memory fallback.", exc)
            mongo_db = None
    else:
        logger.info("motor absent — in-memory mode.")
    try:
        clients = build_client_pool()
        logger.info("EduAgent 7-key client pool ready.")
    except Exception as err:
        logger.error("Client pool failed: %s", err)
        clients = None

    sync_task = asyncio.create_task(_schedule_daily_sync())
    yield
    sync_task.cancel()
    if clients:
        await clients.groq_x.close()
        await clients.groq_y.close()
    clients = None

app = FastAPI(title="EduAgent Enterprise API", version="4.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# HEAVY FILE PARSER (7th Key)
# ---------------------------------------------------------------------------

def split_heavy_document(text: str, max_chars: int = 25000) -> list[str]:
    chunks, pos, total = [], 0, len(text)
    while pos < total:
        end = min(pos + max_chars, total)
        if end < total:
            nb = text.rfind("\n", pos, end)
            if nb > pos + max_chars // 2:
                end = nb + 1
        chunks.append(text[pos:end].strip())
        pos = end
    return chunks

async def run_heavy_parser(pool: ClientPool, text: str) -> str:
    chunks  = split_heavy_document(text)
    results = []
    for idx, chunk in enumerate(chunks):
        summary = await gemini_generate(
            pool.gemini_heavy_parser, GEMINI_MODEL_PARSER,
            f"Extract definitions, formulas, vocabulary:\n\n{chunk}",
            "You extract structured academic data from raw text.")
        results.append(f"[Segment {idx+1}]\n{summary}")
    return "\n\n".join(results)

# ---------------------------------------------------------------------------
# AI PIPELINE
# ---------------------------------------------------------------------------

async def gemini_generate(client, model, prompt, system_instruction=None) -> str:
    config = None
    if system_instruction:
        config = genai.types.GenerateContentConfig(system_instruction=system_instruction)
    r = await client.aio.models.generate_content(model=model, contents=prompt, config=config)
    return (r.text or "").strip()

async def groq_generate(client, model, prompt, system) -> str:
    c = await client.chat.completions.create(
        model=model,
        messages=[{"role":"system","content":system},{"role":"user","content":prompt}],
        temperature=0.4, max_tokens=1024,
    )
    return (c.choices[0].message.content or "").strip()

async def timed_blast(coro, source: str) -> BlastResult:
    t = time.perf_counter()
    try:
        text = await coro
        return BlastResult(source=source, text=text, elapsed_ms=(time.perf_counter()-t)*1000)
    except Exception as exc:
        return BlastResult(source=source, text="", elapsed_ms=(time.perf_counter()-t)*1000, error=str(exc))

async def run_parallel_blast(pool: ClientPool, student: dict, subject: str, query: str):
    sx = "Give a concise, accurate first-pass answer. Be exam-safe and structured."
    sy = "Respond in sections: Definition, Key Points, Example, Common Mistakes."
    tasks = await asyncio.gather(
        timed_blast(gemini_generate(pool.gemini_researcher, GEMINI_MODEL_RESEARCH,
            f"Student: {student['name']} | Marks: {student['metrics'].get('python_marks',0)}\n"
            f"Subject: {subject}\nQ: {query}", "You personalize tutoring based on student profile."), "gemini_b"),
        timed_blast(groq_generate(pool.groq_x, GROQ_MODEL_X, f"Subject {subject}. Q: {query}", sx), "groq_x"),
        timed_blast(groq_generate(pool.groq_y, GROQ_MODEL_Y, f"Subject {subject}. Q: {query}", sy), "groq_y"),
        timed_blast(gemini_generate(pool.gemini_blaster, GEMINI_MODEL_BLAST,
            f"Subject {subject}. Detailed answer:\n{query}", "You are a thorough academic explainer."), "gemini_c"),
    )
    return tasks[0].text or f"Student {student['name']} prefers mixed learning.", list(tasks[1:])

async def judge_with_failover(pool: ClientPool, profile: str, query: str, blasts: list[BlastResult]) -> str:
    candidates = "\n\n".join(f"### {r.source}\n{r.text or '[FAILED]'}" for r in blasts)
    system = (f"You are The Supreme Judge for EduAgent.\nStudent context: {profile}\n"
              f"Rules: 1) Keep only correct content. 2) Lead with practical example. "
              f"3) Return ONE clean exam-ready answer.")
    prompt = f"Question: {query}\n\nCandidates:\n{candidates}"
    try:
        return await gemini_generate(pool.gemini_judge_primary, GEMINI_MODEL_JUDGE, prompt, system)
    except Exception as exc:
        if "429" in str(exc) or "resource_exhausted" in str(exc).lower():
            logger.warning("Primary Judge rate-limited — failing over to backup key.")
            return await gemini_generate(pool.gemini_judge_backup, GEMINI_MODEL_JUDGE, prompt, system)
        raise

def pick_fastest(blasts: list[BlastResult]) -> str:
    hits = [r for r in blasts if r.text and not r.error]
    if hits:
        return min(hits, key=lambda r: r.elapsed_ms).text
    raise RuntimeError("All blast engines failed.")

async def run_academic_pipeline(pool: ClientPool, student: dict, subject: str, query: str):
    profile, blasts = await run_parallel_blast(pool, student, subject, query)
    meta = {"blast_timings_ms": {r.source: round(r.elapsed_ms,1) for r in blasts}, "synthesis": "judge"}
    try:
        return await judge_with_failover(pool, profile, query, blasts), meta
    except Exception:
        meta["synthesis"] = "fail_safe_groq"
        return pick_fastest(blasts), meta

# ---------------------------------------------------------------------------
# STUDENT HELPERS
# ---------------------------------------------------------------------------

def _flatten_student(raw: dict, roll: str) -> dict:
    p = raw.get("profile", {})
    ac = raw.get("academics", {})
    fi = raw.get("finance", {})
    if "attendance" in raw and isinstance(raw["attendance"], dict) and "name" in raw:
        att_vals = [int(v.rstrip("%")) for v in raw.get("attendance", {}).values()]
        avg_att = sum(att_vals) // len(att_vals) if att_vals else 0
        it = raw.get("marks", {})
        py_marks = it.get("Python Programming") or (sum(it.values()) // len(it) if it else 0)
        
        fee_data = raw.get("fee", {})
        fee_status = "Fully Paid" if fee_data.get("due", 0) <= 0 else "Balance Due"
        timetable = {}
        for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
            sh = sorted(list(raw.get("attendance", {}).keys()))
            if len(sh) >= 3:
                timetable[day] = f"{sh[0]}, {sh[1]}, {sh[2]}"
            elif sh:
                timetable[day] = ", ".join(sh)
            else:
                timetable[day] = "No classes"
        return {
            "roll_number": roll,
            "name": raw.get("name", roll),
            "learning_style": "mixed",
            "metrics": {
                "attendance_percent": avg_att,
                "python_marks": py_marks,
                "fee_status": fee_status,
                "present_days": int(200 * avg_att / 100),
                "absent_days": 200 - int(200 * avg_att / 100),
                "overall_grade": "A" if py_marks >= 80 else "B",
                "timetable": timetable,
                "raw_attendance": raw.get("attendance", {}),
            }
        }
    att_vals    = [int(v.rstrip("%")) for v in ac.get("attendance", {}).values()]
    avg_att     = sum(att_vals) // len(att_vals) if att_vals else 0
    it          = ac.get("institution_test", {})
    py_marks    = it.get("Python Programming") or (sum(it.values()) // len(it) if it else 0)
    grades      = list(ac.get("university_marks", {}).values())
    total_days  = 200
    return {
        "roll_number":    roll,
        "name":           p.get("name", roll),
        "learning_style": "mixed",
        "metrics": {
            "attendance_percent": avg_att,
            "python_marks":       py_marks,
            "fee_status":         fi.get("status", "Unknown"),
            "present_days":       int(total_days * avg_att / 100),
            "absent_days":        total_days - int(total_days * avg_att / 100),
            "overall_grade":      grades[0].split()[0] if grades else "B",
            "timetable":          ac.get("timetable", {}),
            "raw_attendance":     ac.get("attendance", {}),
        },
    }

async def _build_personal_response(student: dict, query: str) -> str:
    m = student["metrics"]
    q = query.lower()
    roll = student["roll_number"]
    if not m.get("raw_attendance") and m.get("attendance_percent") == 0:
        fresh_erp = await fetch_from_erp(f"/student/{roll}")
        if fresh_erp:
            student = _flatten_student(fresh_erp, roll)
            m = student["metrics"]
    if re.search(r"\battendance|present|absent\b", q):
        return (f"📊 Attendance — {student['roll_number']} ({student['name']}):\n"
                f"Overall: {m['attendance_percent']}% | "
                f"Present: {m['present_days']} days | Absent: {m['absent_days']} days")
    if re.search(r"\bmark|marks|score|grade\b", q):
        return (f"📝 Marks — {student['roll_number']}:\n"
                f"Python: {m['python_marks']}/100 | Grade: {m['overall_grade']}")
    if re.search(r"\bfee|fees\b", q):
        return f"💳 Fee status — {student['roll_number']}: {m['fee_status']}"
    if re.search(r"\btimetable|schedule\b", q):
        tt = m.get("timetable", {})
        lines = "\n".join(f"{d}: {s}" for d, s in tt.items())
        return f"📅 Timetable — {student['roll_number']}:\n{lines}"
    return (f"👤 {student['roll_number']} ({student['name']}) — "
            f"Att: {m['attendance_percent']}% | Marks: {m['python_marks']} | Fee: {m['fee_status']}")

def _build_low_att_warning(student: dict) -> str | None:
    raw_att = student["metrics"].get("raw_attendance", {})
    danger  = []
    for sub, pct in raw_att.items():
        val = int(pct.rstrip("%")) if isinstance(pct, str) else int(pct)
        if val < LOW_ATT_WARNING_PCT:
            danger.append((sub, val))
    if not danger:
        return None
    lines = "\n".join(f"  ⚠️  {sub}: {pct}% (minimum 75%)" for sub, pct in danger)
    return (f"🚨 Attendance Warning for {student['roll_number']}:\n"
            f"The following subjects are approaching the shortage limit:\n{lines}\n"
            f"Please attend classes regularly to avoid exam detainment.")

async def _run_eduagent_query(roll: str, subject: str, query: str) -> AskEduAgentResponse:
    t0        = time.perf_counter()
    user_data = await db_get_user(roll)
    if user_data and user_data.get("role") == "faculty":
        return AskEduAgentResponse(
            track="personal", roll_number=roll, subject_code=subject,
            answer=f"Hello Professor {user_data['profile']['name']}. How can I assist today?",
            latency_ms=(time.perf_counter()-t0)*1000, meta={"role": "faculty"})
    student = _flatten_student(user_data or {}, roll)
    track   = "personal" if PERSONAL_KEYWORDS.search(query) else "academic"
    if track == "personal":
        personal_ans = await _build_personal_response(student, query)
        return AskEduAgentResponse(
            track="personal", roll_number=roll, subject_code=subject,
            answer=personal_ans,
            latency_ms=(time.perf_counter()-t0)*1000, meta={"router": "local_keyword_match"})
    if clients is None:
        raise HTTPException(status_code=503, detail="AI Client Pool Offline.")
    answer, meta = await run_academic_pipeline(clients, student, subject, query)
    return AskEduAgentResponse(
        track="academic", roll_number=roll, subject_code=subject,
        answer=answer, latency_ms=round((time.perf_counter()-t0)*1000, 2), meta=meta)

# ---------------------------------------------------------------------------
# HTTP ROUTES
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "version": "4.0.0",
            "database": "mongodb" if mongo_db is not None else "in-memory",
            "ai_pool":  "online"  if clients  is not None else "offline"}

@app.post("/student/sync")
async def student_sync(payload: StudentSyncRequest) -> dict:
    roll_number = payload.roll_number.strip().upper()
    erp_data = await fetch_from_erp(f"/student/{roll_number}")
    if erp_data:
        student_record = erp_data
    else:
        student_record = IN_MEMORY_DB.get(roll_number)
    if not student_record or (isinstance(student_record, dict) and student_record.get("role") == "faculty"):
        raise HTTPException(status_code=404, detail=f"Student {roll_number} not found in sandbox or live ERP.")
    await db_save_snapshot(roll_number, student_record)
    await db_update_last_seen(roll_number)
    student = _flatten_student(student_record, roll_number)
    warning = _build_low_att_warning(student)
    logger.info("Synced %s to MongoDB snapshot caches.", roll_number)
    return {
        "status":   "synced",
        "roll":     roll_number,
        "synced_at": time.time(),
        "warning":  warning,
    }

@app.get("/student/snapshot/{roll_number}")
async def get_student_snapshot(roll_number: str) -> dict:
    roll = roll_number.strip().upper()
    doc  = await db_get_snapshot(roll)
    if not doc:
        raise HTTPException(status_code=404, detail=f"No snapshot found for {roll}. Student has not logged in yet.")
    snap = doc.get("snapshot", {})
    return {
        "roll":      roll,
        "name":      snap.get("profile", {}).get("name", snap.get("name", roll)),
        "dept":      snap.get("profile", {}).get("department", snap.get("department", "—")),
        "synced_at": doc.get("synced_at"),
        "finance":   snap.get("finance", snap.get("fee", {})),
        "academics": snap.get("academics", {"attendance": snap.get("attendance", {}), "institution_test": snap.get("marks", {})}),
    }

@app.get("/student/sync-status/{roll_number}")
async def get_sync_status(roll_number: str) -> dict:
    roll = roll_number.strip().upper()
    doc  = await db_get_snapshot(roll)
    if not doc:
        return {"roll": roll, "status": "never_synced", "last_synced_at": None, "changes": []}
    fresh   = IN_MEMORY_DB.get(roll, {})
    changes = _detect_changes(doc.get("snapshot", {}), fresh) if fresh else []
    synced_ts = doc.get("synced_at")
    synced_dt = datetime.fromtimestamp(synced_ts).strftime("%d %b %Y, %I:%M %p") if synced_ts else "Unknown"
    return {
        "roll":          roll,
        "status":        "up_to_date" if not changes else "changes_detected",
        "last_synced_at": synced_dt,
        "changes":       changes,
    }

@app.post("/ask-eduagent", response_model=AskEduAgentResponse)
async def ask_eduagent(payload: AskEduAgentRequest) -> AskEduAgentResponse:
    roll    = payload.roll_number.strip().upper()
    subject = payload.subject_code.strip().upper()
    query   = payload.student_query.strip()
    result  = await _run_eduagent_query(roll, subject, query)
    await db_save_message("private_messages", roll, "private",
                          {"role": "user",  "text": query, "ts": time.time()})
    await db_save_message("private_messages", roll, "private",
                          {"role": "agent", "text": result.answer, "track": result.track, "ts": time.time()})
    return result

@app.get("/history/{roll_number}")
async def get_history(roll_number: str, limit: int = 50) -> dict:
    roll = roll_number.strip().upper()
    return {"roll_number": roll,
            "messages": await db_load_history("private_messages", roll, "private", limit)}

@app.post("/faculty/upload-materials")
async def upload_materials(payload: UploadMaterialRequest) -> dict:
    if clients is None:
        raise HTTPException(status_code=503, detail="AI Infrastructure Offline.")
    body = payload.body_text
    if len(body) > 40000:
        logger.info("Heavy document detected — activating 7th Key parser.")
        body = await run_heavy_parser(clients, body)
    await db_save_study_material(payload.subject_code.upper(), payload.unit, payload.heading, body)
    return {"status": "success", "message": "Material uploaded."}

@app.get("/student/materials/{subject_code}")
async def get_materials(subject_code: str) -> dict:
    return {"subject_code": subject_code.upper(),
            "materials": await db_load_study_materials(subject_code.upper())}

@app.post("/room/create", response_model=CreateRoomResponse)
async def create_room() -> CreateRoomResponse:
    code = room_manager.create_room()
    return CreateRoomResponse(room_code=code)

@app.get("/room/{room_code}/exists")
async def room_exists(room_code: str) -> dict:
    return {"exists": room_manager.room_exists(room_code.upper())}

@app.get("/room/{room_code}/history")
async def room_history(room_code: str, limit: int = 50) -> dict:
    return {"room": room_code.upper(),
            "messages": await db_load_room_history(room_code.upper(), limit=limit)}

@app.post("/faculty/trigger-attendance-window")
async def trigger_attendance_window(payload: AttendanceTokenRequest) -> dict:
    room_code = payload.room_code.upper()
    if not room_manager.room_exists(room_code):
        room_manager.rooms[room_code] = []
    faculty_session_manager.start_attendance_session(room_code, payload.duration_seconds)
    return {"status": "success", "room_code": room_code, "expires_in_secs": payload.duration_seconds}

@app.post("/faculty/broadcast-mass-notice")
async def broadcast_mass_notice(payload: MassNotificationRequest) -> dict:
    dispatched = 0
    for roll in payload.target_student_rolls:
        roll = roll.strip().upper()
        sock = active_campus_sockets.get(roll)
        if sock:
            try:
                await sock.send_json({
                    "type": "agent_reply",
                    "text": f"📢 Faculty Notice:\n\n{payload.announcement_message}",
                    "track": "personal", "ts": time.time(),
                })
                dispatched += 1
            except Exception:
                pass
    return {"status": "success", "total_targets": len(payload.target_student_rolls),
            "live_dispatched": dispatched}

# ---------------------------------------------------------------------------
# WEBSOCKET — GROUP CHAT HUB
# ---------------------------------------------------------------------------

@app.websocket("/ws/chat/{room_code}")
async def websocket_group_chat(websocket: WebSocket, room_code: str):
    room_code = room_code.upper()
    roll      = websocket.query_params.get("roll", "").upper()
    subject   = websocket.query_params.get("subject", "24ITE01").upper()

    if not roll:
        await websocket.close(code=4001); return

    if not faculty_session_manager.is_session_valid(room_code):
        await websocket.close(code=4041); return

    clean_roll = roll.replace("FACULTY_", "")
    user_data  = await db_get_user(clean_roll)
    if not user_data:
        await websocket.close(code=4004); return

    display_name = user_data.get("profile", {}).get("name", roll)
    is_prof      = user_data.get("role") == "faculty" or roll.startswith("FACULTY_")

    if not room_manager.room_exists(room_code):
        room_manager.rooms[room_code] = []

    await websocket.accept()
    member = RoomMember(websocket=websocket, roll=roll, name=display_name)
    await room_manager.connect(room_code, member)
    active_campus_sockets[roll] = websocket

    history = await db_load_room_history(room_code, limit=50)
    if history:
        await websocket.send_json({"type": "history", "messages": history})

    await room_manager.broadcast(room_code, {
        "type": "system",
        "text": f"📌 {'Professor ' if is_prof else ''}{display_name} joined the room.",
        "ts": time.time(),
    }, exclude=websocket)

    await room_manager.broadcast_all(room_code, {
        "type": "members", "members": room_manager.member_list(room_code)
    })

    try:
        while True:
            data     = await websocket.receive_json()
            msg_type = data.get("type", "chat")

            if msg_type == "chat":
                text = (data.get("text") or "").strip()
                if not text:
                    continue
                out = {"type":"chat","roll":roll,"name":display_name,"text":text,"ts":time.time()}
                await db_save_group_message(room_code, out)
                await room_manager.broadcast_all(room_code, out)

            elif msg_type == "agent":
                query = (data.get("text") or "").strip()
                if not query:
                    continue
                q_msg = {"type":"agent_question","roll":roll,"name":display_name,
                         "text":f"/agent {query}","ts":time.time()}
                await db_save_group_message(room_code, q_msg)
                await room_manager.broadcast_all(room_code, q_msg)
                await room_manager.broadcast_all(room_code,
                    {"type":"agent_typing","text":"EduAgent is synthesizing…"})

                if clients is None:
                    reply = {"type":"agent_reply","text":"⚠️ AI pool offline.","track":"error","ts":time.time()}
                else:
                    try:
                        mock = {"name": display_name, "learning_style": "mixed",
                                "metrics": {"python_marks": 85}}
                        answer, meta = await run_academic_pipeline(clients, mock, subject, query)
                        reply = {"type":"agent_reply","text":answer,"track":"academic",
                                 "latency_ms":round(sum(meta["blast_timings_ms"].values()),1),
                                 "ts":time.time()}
                    except Exception as exc:
                        reply = {"type":"agent_reply","text":f"⚠️ Error: {exc}","track":"error","ts":time.time()}

                await db_save_group_message(room_code, reply)
                await room_manager.broadcast_all(room_code, reply)

    except WebSocketDisconnect:
        room_manager.disconnect(room_code, member)
        active_campus_sockets.pop(roll, None)
        await room_manager.broadcast(room_code,
            {"type":"system","text":f"👋 {display_name} left the room.","ts":time.time()})
        await room_manager.broadcast_all(room_code,
            {"type":"members","members":room_manager.member_list(room_code)})
