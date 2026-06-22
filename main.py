from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid
import hashlib
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
# MODEL CONFIG & ROUTING KEYWORDS
# ---------------------------------------------------------------------------

GEMINI_MODEL_RESEARCH = os.getenv("GEMINI_MODEL_RESEARCH", "gemini-2.0-flash")
GEMINI_MODEL_BLAST    = os.getenv("GEMINI_MODEL_BLAST",    "gemini-2.0-flash")
GEMINI_MODEL_JUDGE    = os.getenv("GEMINI_MODEL_JUDGE",    "gemini-2.0-flash")
GEMINI_MODEL_PARSER   = os.getenv("GEMINI_MODEL_PARSER",   "gemini-2.0-flash")
GROQ_MODEL_X          = os.getenv("GROQ_MODEL_X",          "llama-3.1-8b-instant")
GROQ_MODEL_Y          = os.getenv("GROQ_MODEL_Y",          "mixtral-8x7b-32768")

PERSONAL_KEYWORDS = re.compile(
    r"\b(attendance|mark|marks|score|grade|present|absent|fee|fees|timetable|time\s+table|schedule|work|today|class|classes|subject|subjects|balance|paid|due|assignment|assignments|paper|papers)\b",
    re.IGNORECASE,
)

LOW_ATT_WARNING_PCT = 78

WK_ERP_BASE  = os.getenv("WK_ERP_BASE",  "https://white-knights-erp.onrender.com")
WK_ERP_TOKEN = os.getenv("WK_ERP_TOKEN", "WK_DEV_TOKEN_2025")

# ---------------------------------------------------------------------------
# MONGODB LAYER WITH SECURE CLOUD FALLBACKS
# ---------------------------------------------------------------------------

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/eduagent")
mongo_db  = None

FALLBACK_PRIVATE_HISTORY:  dict[str, list[dict]] = defaultdict(list)
FALLBACK_GROUP_HISTORY:    dict[str, list[dict]] = defaultdict(list)
FALLBACK_STUDY_MATERIALS:  list[dict]            = []
FALLBACK_SNAPSHOTS:        dict[str, dict]       = {}

try:
    import motor.motor_asyncio as motor_asyncio
    MOTOR_AVAILABLE = True
except ImportError:
    MOTOR_AVAILABLE = False
    logger.warning("motor not installed — running in-memory fallback snapshot mode.")

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
            if "FAC_" in token_id or "WK" in token_id or "DR_" in token_id or "PROF_" in token_id:
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
        await mongo_db[coll].insert_one({"roll_number": roll, "room": room, "message": message, "ts": time.time()})
    except Exception:
        FALLBACK_PRIVATE_HISTORY[roll].append(message)

async def db_load_history(coll: str, roll: str, room: str, limit: int = 50) -> list[dict]:
    if mongo_db is None:
        return FALLBACK_PRIVATE_HISTORY[roll][-limit:]
    try:
        cursor = mongo_db[coll].find({"roll_number": roll, "room": room}, {"_id": 0, "message": 1}).sort("ts", -1).limit(limit)
        docs = await cursor.to_list(length=limit)
        return [d["message"] for d in reversed(docs)]
    except Exception:
        return FALLBACK_PRIVATE_HISTORY[roll][-limit:]

async def db_load_room_history(room: str, limit: int = 100) -> list[dict]:
    if mongo_db is None:
        return FALLBACK_GROUP_HISTORY[room][-limit:]
    try:
        cursor = mongo_db["group_messages"].find({"room": room}, {"_id": 0, "message": 1}).sort("ts", -1).limit(limit)
        docs = await cursor.to_list(length=limit)
        return [d["message"] for d in reversed(docs)]
    except Exception:
        return FALLBACK_GROUP_HISTORY[room][-limit:]

async def db_save_group_message(room: str, message: dict) -> None:
    if mongo_db is None:
        FALLBACK_GROUP_HISTORY[room].append(message)
        return
    try:
        await mongo_db["group_messages"].insert_one({"room": room, "message": message, "ts": time.time()})
    except Exception:
        FALLBACK_GROUP_HISTORY[room].append(message)

async def db_save_study_material(subject_code: str, unit: str, heading: str, body: str) -> None:
    doc = {"subject_code": subject_code, "unit": unit, "heading": heading, "body": body, "ts": time.time()}
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
        cursor = mongo_db["study_materials"].find({"subject_code": subject_code}, {"_id": 0}).sort("ts", -1)
        return await cursor.to_list(length=100)
    except Exception:
        return [m for m in FALLBACK_STUDY_MATERIALS if m["subject_code"] == subject_code]

async def db_save_snapshot(roll: str, snapshot: dict) -> None:
    doc = {"roll_number": roll, "snapshot": snapshot, "synced_at": time.time()}
    if mongo_db is None:
        FALLBACK_SNAPSHOTS[roll] = doc
        return
    try:
        await mongo_db["student_snapshots"].update_one({"roll_number": roll}, {"$set": doc}, upsert=True)
    except Exception:
        FALLBACK_SNAPSHOTS[roll] = doc

async def db_get_snapshot(roll: str) -> dict | None:
    if mongo_db is None:
        return FALLBACK_SNAPSHOTS.get(roll)
    try:
        return await mongo_db["student_snapshots"].find_one({"roll_number": roll}, {"_id": 0})
    except Exception:
        return FALLBACK_SNAPSHOTS.get(roll)

async def db_update_last_seen(roll: str) -> None:
    doc = {"roll_number": roll, "last_seen": time.time()}
    if mongo_db is None:
        FALLBACK_SNAPSHOTS.setdefault(roll, {})["last_seen_ts"] = time.time()
        return
    try:
        await mongo_db["last_seen"].update_one({"roll_number": roll}, {"$set": doc}, upsert=True)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# OUTBOUND NETWORK CONNECTIVITY TIMEOUT COUPLING
# ---------------------------------------------------------------------------

async def fetch_from_erp(path: str) -> Optional[dict]:
    url = f"{WK_ERP_BASE.rstrip('/')}{path}"
    headers = {"X-System-Token": WK_ERP_TOKEN}
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            logger.warning("ERP service returned status %d for path: %s", response.status_code, path)
    except Exception as exc:
        logger.error("Outbound ERP layer timeout handshake failed: %s", exc)
    return None

# ---------------------------------------------------------------------------
# 7-KEY CLIENT POOL MANAGEMENT
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
        raise RuntimeError(f"Missing absolute key reference parameter: {name}")
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
# BRICK 2 — DAILY AUTO-SYNC TRACKING LOGIC
# ---------------------------------------------------------------------------

def _detect_changes(old_snap: dict, new_data: dict) -> list[str]:
    changes: list[str] = []
    old_acc = old_snap.get("academics", {})
    new_acc = new_data.get("academics", {})

    old_att = old_acc.get("attendance", {})
    new_att = new_data.get("attendance", new_acc.get("attendance", {}))
    
    for sub, new_pct in new_att.items():
        try:
            old_val = int(old_att.get(sub, "0").rstrip("%"))
            new_val = int(str(new_pct).rstrip("%"))
            if new_val != old_val:
                direction = "📉 dropped" if new_val < old_val else "📈 improved"
                changes.append(f"Attendance in {sub} {direction}: {old_val}% → {new_val}%")
        except Exception:
            pass

    old_it = old_acc.get("institution_test", {})
    new_it = new_data.get("marks", new_acc.get("institution_test", {}))
    for sub, new_score in new_it.items():
        old_score = old_it.get(sub, 0)
        if new_score != old_score:
            changes.append(f"Marks updated in {sub}: {old_score} → {new_score}")

    return changes

async def _sync_one_student(roll: str) -> list[str]:
    fresh = await fetch_from_erp(f"/student/{roll}")
    if not fresh:
        return []

    old_doc = await db_get_snapshot(roll)
    changes: list[str] = []
    if old_doc:
        changes = _detect_changes(old_doc.get("snapshot", {}), fresh)
    await db_save_snapshot(roll, fresh)
    return changes

async def _daily_sync_all_students() -> None:
    logger.info("🔄 Initiating daily cloud auto-sync cycle...")
    if mongo_db is not None:
        cursor = mongo_db["student_snapshots"].find({}, {"roll_number": 1})
        student_rolls = [d["roll_number"] for d in await cursor.to_list(length=2000)]
    else:
        student_rolls = list(FALLBACK_SNAPSHOTS.keys())

    total_changes = 0
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
                        "text":  f"🔔 ERP Sync Update Detected:\n\n{change_text}",
                        "track": "personal",
                        "ts":    time.time(),
                    })
                except Exception:
                    pass
    logger.info("✅ Daily cloud sync validation engine complete. Sync updates: %d.", total_changes)

async def _schedule_daily_sync() -> None:
    while True:
        now = datetime.now()
        target = now.replace(hour=16, minute=30, second=0, microsecond=0)
        if now >= target:
            target = target + timedelta(days=1)
        wait_secs = (target - now).total_seconds()
        logger.info("⏰ Auto-sync schedule verified. Sync loop asleep for %.0f seconds.", wait_secs)
        await asyncio.sleep(wait_secs)
        await _daily_sync_all_students()

# ---------------------------------------------------------------------------
# LIFESPAN INITIALIZATION
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global clients, mongo_db
    if MOTOR_AVAILABLE:
        try:
            mc       = motor_asyncio.AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            mongo_db = mc["eduagent"]
            await mongo_db["private_messages"].create_index([("roll_number",1),("room",1),("ts",-1)])
            await mongo_db["group_messages"].create_index([("room",1),("ts",-1)])
            await mongo_db["study_materials"].create_index([("subject_code",1),("ts",-1)])
            await mongo_db["student_snapshots"].create_index([("roll_number",1)], unique=True)
            await mongo_db["last_seen"].create_index([("roll_number",1)], unique=True)
            logger.info("Production MongoDB cluster connection established.")
        except Exception as exc:
            logger.warning("MongoDB cluster refused handshake (%s) — using RAM fallbacks.", exc)
            mongo_db = None
    else:
        logger.info("Motor library mapping absent — dynamic memory layer activated.")

    try:
        clients = build_client_pool()
        logger.info("7-Key enterprise model cluster array safely loaded.")
    except Exception as err:
        logger.error("Critical parallel API key compilation mapping error: %s", err)
        clients = None

    sync_task = asyncio.create_task(_schedule_daily_sync())
    yield
    sync_task.cancel()
    if clients:
        await clients.groq_x.close()
        await clients.groq_y.close()
    clients = None

app = FastAPI(title="EduAgent Production AI Cluster", version="4.0.0", lifespan=lifespan)
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
            f"Extract academic text definitions, materials data, formulas, or logs:\n\n{chunk}",
            "You parse administrative and academic documentation structural layouts.")
        results.append(f"[Segment {idx+1}]\n{summary}")
    return "\n\n".join(results)

# ---------------------------------------------------------------------------
# AI CONCURRENCY BALANCING AND JUDGE PIPELINE
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
    sx = "Provide an expert, highly accurate tutoring answer. Keep layout clean and structured."
    sy = "Synthesize text into clear blocks: Core Concepts, Implementation Rules, Practical Examples."
    tasks = await asyncio.gather(
        timed_blast(gemini_generate(pool.gemini_researcher, GEMINI_MODEL_RESEARCH,
            f"Student Profile: {student['name']} | Track Grade: {student['metrics'].get('python_marks',80)}\n"
            f"Subject Context: {subject}\nRequest: {query}", "You personalize tutoring based on student profiles."), "gemini_b"),
        timed_blast(groq_generate(pool.groq_x, GROQ_MODEL_X, f"Subject: {subject}. Request: {query}", sx), "groq_x"),
        timed_blast(groq_generate(pool.groq_y, GROQ_MODEL_Y, f"Subject: {subject}. Request: {query}", sy), "groq_y"),
        timed_blast(gemini_generate(pool.gemini_blaster, GEMINI_MODEL_BLAST,
            f"Subject: {subject}. Comprehensive Request Evaluation:\n{query}", "You are an exhaustive academic domain explainer."), "gemini_c"),
    )
    return tasks[0].text or f"Student {student['name']} processing sequence complete.", list(tasks[1:])

async def judge_with_failover(pool: ClientPool, profile: str, query: str, blasts: list[BlastResult]) -> str:
    candidates = "\n\n".join(f"### Agent Output Channel [{r.source}]\n{r.text or '[TIMEOUT/FAILED]'}" for r in blasts)
    system = (f"You are the Supreme Judge Core for the EduAgent system cluster.\nStudent metadata: {profile}\n"
              f"Rules: 1) Eliminate erroneous answers. 2) Lead with a clean structure. 3) Output ONE comprehensive answer.")
    prompt = f"Target Query: {query}\n\nAvailable Candidates:\n{candidates}"
    try:
        return await gemini_generate(pool.gemini_judge_primary, GEMINI_MODEL_JUDGE, prompt, system)
    except Exception as exc:
        if "429" in str(exc) or "resource" in str(exc).lower():
            logger.warning("Primary Judge execution core exhausted. Activating fallback pipeline.")
            return await gemini_generate(pool.gemini_judge_backup, GEMINI_MODEL_JUDGE, prompt, system)
        raise

def pick_fastest(blasts: list[BlastResult]) -> str:
    hits = [r for r in blasts if r.text and not r.error]
    if hits:
        return min(hits, key=lambda r: r.elapsed_ms).text
    raise RuntimeError("Total AI execution pipeline failure encountered.")

async def run_academic_pipeline(pool: ClientPool, student: dict, subject: str, query: str):
    profile, blasts = await run_parallel_blast(pool, student, subject, query)
    meta = {"blast_timings_ms": {r.source: round(r.elapsed_ms,1) for r in blasts}, "synthesis": "supreme_judge"}
    try:
        return await judge_with_failover(pool, profile, query, blasts), meta
    except Exception:
        meta["synthesis"] = "speed_fail_safe_fallback"
        return pick_fastest(blasts), meta

# ---------------------------------------------------------------------------
# CORE PROFILE TRANSFORM MATCHERS
# ---------------------------------------------------------------------------

def _flatten_student(raw: dict, roll: str) -> dict:
    if "attendance" in raw or "name" in raw:
        attendance_dict = raw.get("attendance", {})
        att_vals = []
        for v in attendance_dict.values():
            try:
                att_vals.append(int(str(v).rstrip("%")))
            except ValueError:
                pass
        avg_att = sum(att_vals) // len(att_vals) if att_vals else 0
        
        marks_dict = raw.get("marks", {})
        py_marks = marks_dict.get("Python Programming") or marks_dict.get("Python") or (sum(marks_dict.values()) // len(marks_dict) if marks_dict else 0)
        
        fee_data = raw.get("fee", {})
        fee_status = "Fully Paid" if fee_data.get("due", 0) <= 0 else f"Balance Due: {fee_data.get('due')}"
        
        timetable = {}
        for day in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]:
            sh = list(attendance_dict.keys())
            if sh:
                seed_str = f"{day}{roll}"
                hash_hex = hashlib.md5(seed_str.encode()).hexdigest()
                seed_int = int(hash_hex, 16) & 0xFFFFFFFF
                shift = seed_int % len(sh)
                rotated = sh[shift:] + sh[:shift]
                timetable[day] = ", ".join(rotated[:3])
            else:
                timetable[day] = "No sessions registered"

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
                "overall_grade": "A+" if py_marks >= 80 else "B",
                "timetable": timetable,
                "raw_attendance": attendance_dict,
                "raw_marks": marks_dict
            }
        }
    return {
        "roll_number":    roll,
        "name":           "Student Asset",
        "learning_style": "mixed",
        "metrics": {
            "attendance_percent": 0, "python_marks": 0, "fee_status": "Unverified",
            "present_days": 0, "absent_days": 0, "overall_grade": "B",
            "timetable": {}, "raw_attendance": {}, "raw_marks": {}
        },
    }

async def _build_personal_response(student: dict, query: str) -> str:
    m = student["metrics"]
    q = query.lower()
    
    if re.search(r"\battendance|present|absent\b", q):
        lines = "\n".join(f"  • {sub}: {pct}" for sub, pct in m["raw_attendance"].items())
        return (f"📊 Attendance Breakdown — {student['roll_number']} ({student['name']}):\n"
                f"Overall Average: {m['attendance_percent']}% | Present: {m['present_days']} Days | Absent: {m['absent_days']} Days\n\n"
                f"Subject Metrics:\n{lines}")
                
    if re.search(r"\bmark|marks|score|grade\b", q):
        lines = "\n".join(f"  • {sub}: {score}/100" for sub, score in m["raw_marks"].items())
        return (f"📝 Academic Report Snapshot — {student['roll_number']}:\n"
                f"Overall Grade Profile: {m['overall_grade']}\n\n"
                f"Subject Performance Summary:\n{lines}")
                
    if re.search(r"\bfee|fees|balance|paid|due\b", q):
        return f"💳 Financial Status Verification — {student['roll_number']}:\nLedger Status: {m['fee_status']}"
        
    if re.search(r"\btimetable|schedule|work|today|class|classes\b", q):
        tt = m.get("timetable", {})
        lines = "\n".join(f"  • {d}: {s}" for d, s in tt.items())
        return f"📅 Active Room Schedule — {student['roll_number']}:\n{lines}"

    if re.search(r"\bmaterial|materials|assignment|assignments|paper|papers\b", q):
        return f"📚 Reference Documentation — Check the 'Course Vault' drawer inside your student desktop workspace hub to download resources uploaded by your instructors."

    return (f"👤 Profile Record Master — {student['roll_number']} ({student['name']}):\n"
            f"Cumulative Attendance: {m['attendance_percent']}% | Exam Average: {m['python_marks']}/100 | Ledger Status: {m['fee_status']}")

def _build_low_att_warning(student: dict) -> str | None:
    raw_att = student["metrics"].get("raw_attendance", {})
    danger = []
    for sub, pct_str in raw_att.items():
        try:
            val = int(str(pct_str).rstrip("%"))
            if val < LOW_ATT_WARNING_PCT:
                danger.append((sub, val))
        except ValueError:
            pass
    if not danger:
        return None
    lines = "\n".join(f"  ⚠️  {sub}: {pct}% (required threshold: 75%)" for sub, pct in danger)
    return (f"🚨 Institutional Attendance Alert Alert:\n"
            f"Your current logs inside the following tracks are dropping below safe standards:\n{lines}\n"
            f"Please optimize attendance intervals immediately to prevent exam detachment constraints.")

async def _run_eduagent_query(roll: str, subject: str, query: str) -> AskEduAgentResponse:
    t0 = time.perf_counter()
    
    # 1. High priority identity validation intercept for Faculty channels
    if "FAC_" in roll or "DR_" in roll or "PROF_" in roll or len(roll) < 7:
        user_data = await db_get_user(roll)
        name = user_data["profile"]["name"] if user_data else "Faculty Member"
        return AskEduAgentResponse(
            track="personal", roll_number=roll, subject_code=subject,
            answer=f"Hello Professor {name}. Your classroom orchestration systems are online. How can I assist you managing today?",
            latency_ms=(time.perf_counter()-t0)*1000, meta={"role": "faculty"})

    # 2. Student processing queue sequence logic execution
    snapshot_doc = await db_get_snapshot(roll)
    raw_data = snapshot_doc.get("snapshot", {}) if snapshot_doc else {}
    
    if not raw_data:
        fresh = await fetch_from_erp(f"/student/{roll}")
        if fresh:
            raw_data = fresh
            await db_save_snapshot(roll, fresh)

    student = _flatten_student(raw_data, roll)
    track   = "personal" if PERSONAL_KEYWORDS.search(query) else "academic"
    
    if track == "personal":
        personal_ans = await _build_personal_response(student, query)
        return AskEduAgentResponse(
            track="personal", roll_number=roll, subject_code=subject,
            answer=personal_ans,
            latency_ms=(time.perf_counter()-t0)*1000, meta={"router": "local_keyword_match"})
            
    if clients is None:
        raise HTTPException(status_code=503, detail="AI Inference Pipeline Offline.")
        
    answer, meta = await run_academic_pipeline(clients, student, subject, query)
    return AskEduAgentResponse(
        track="academic", roll_number=roll, subject_code=subject,
        answer=answer, latency_ms=round((time.perf_counter()-t0)*1000, 2), meta=meta)

# ---------------------------------------------------------------------------
# HTTP REST ENDPOINTS
# ---------------------------------------------------------------------------

@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "version": "4.0.0",
            "database": "mongodb" if mongo_db is not None else "in-memory-snapshots",
            "ai_pool":  "online"  if clients  is not None else "offline"}

@app.post("/student/sync")
async def student_sync(payload: StudentSyncRequest) -> dict:
    roll_number = payload.roll_number.strip().upper()
    erp_data = await fetch_from_erp(f"/student/{roll_number}")
    if not erp_data:
        raise HTTPException(status_code=404, detail=f"Roll number reference {roll_number} not registered on ERP clusters.")
        
    await db_save_snapshot(roll_number, erp_data)
    await db_update_last_seen(roll_number)          
    student = _flatten_student(erp_data, roll_number)
    warning = _build_low_att_warning(student)
    return {
        "status":    "synced",
        "roll":      roll_number,
        "synced_at": time.time(),
        "warning":   warning,   
    }

@app.get("/student/snapshot/{roll_number}")
async def get_student_snapshot(roll_number: str) -> dict:
    roll = roll_number.strip().upper()
    doc  = await db_get_snapshot(roll)
    if not doc:
        raise HTTPException(status_code=404, detail=f"No stored snapshot profile available for tracking token: {roll}.")
    snap = doc.get("snapshot", {})
    return {
        "roll":      roll,
        "name":      snap.get("name", roll),
        "dept":      snap.get("department", "—"),
        "synced_at": doc.get("synced_at"),
        "finance":   snap.get("fee", {}),
        "academics": {"attendance": snap.get("attendance", {}), "institution_test": snap.get("marks", {})},
    }

@app.get("/student/sync-status/{roll_number}")
async def get_sync_status(roll_number: str) -> dict:
    roll = roll_number.strip().upper()
    doc  = await db_get_snapshot(roll)
    if not doc:
        return {"roll": roll, "status": "never_synced", "last_synced_at": None, "changes": []}
    
    fresh = await fetch_from_erp(f"/student/{roll}")
    changes = _detect_changes(doc.get("snapshot", {}), fresh) if fresh else []
    synced_ts = doc.get("synced_at")
    synced_dt = datetime.fromtimestamp(synced_ts).strftime("%d %b %Y, %I:%M %p") if synced_ts else "Unknown"
    return {
        "roll":           roll,
        "status":         "up_to_date" if not changes else "changes_detected",
        "last_synced_at": synced_dt,
        "changes":        changes,
    }

@app.post("/ask-eduagent", response_model=AskEduAgentResponse)
async def ask_eduagent(payload: AskEduAgentRequest) -> AskEduAgentResponse:
    roll    = payload.roll_number.strip().upper()
    subject = payload.subject_code.strip().upper()
    query   = payload.student_query.strip()
    result  = await _run_eduagent_query(roll, subject, query)
    await db_save_message("private_messages", roll, "private", {"role": "user",  "text": query, "ts": time.time()})
    await db_save_message("private_messages", roll, "private", {"role": "agent", "text": result.answer, "track": result.track, "ts": time.time()})
    return result

@app.get("/history/{roll_number}")
async def get_history(roll_number: str, limit: int = 50) -> dict:
    roll = roll_number.strip().upper()
    return {"roll_number": roll, "messages": await db_load_history("private_messages", roll, "private", limit)}

@app.post("/faculty/upload-materials")
async def upload_materials(payload: UploadMaterialRequest) -> dict:
    if clients is None:
        raise HTTPException(status_code=503, detail="AI Cluster offline.")
    body = payload.body_text
    if len(body) > 40000:
        body = await run_heavy_parser(clients, body)
    await db_save_study_material(payload.subject_code.upper(), payload.unit, payload.heading, body)
    return {"status": "success", "message": "Academic documentation saved to Vault array successfully."}

@app.get("/student/materials/{subject_code}")
async def get_materials(subject_code: str) -> dict:
    return {"subject_code": subject_code.upper(), "materials": await db_load_study_materials(subject_code.upper())}

@app.post("/room/create", response_model=CreateRoomResponse)
async def create_room() -> CreateRoomResponse:
    code = room_manager.create_room()
    return CreateRoomResponse(room_code=code)

@app.get("/room/{room_code}/exists")
async def room_exists(room_code: str) -> dict:
    return {"exists": room_manager.room_exists(room_code.upper())}

@app.get("/room/{room_code}/history")
async def room_history(room_code: str, limit: int = 50) -> dict:
    return {"room": room_code.upper(), "messages": await db_load_room_history(room_code.upper(), limit=limit)}

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
                    "text": f"📢 Direct Faculty Notice:\n\n{payload.announcement_message}",
                    "track": "personal", "ts": time.time(),
                })
                dispatched += 1
            except Exception:
                pass
    return {"status": "success", "total_targets": len(payload.target_student_rolls), "live_dispatched": dispatched}

# ---------------------------------------------------------------------------
# WEBSOCKET REAL-TIME COMMUNICATIONS CHANNELS HIPS
# ---------------------------------------------------------------------------

@app.websocket("/ws/chat/{room_code}")
async def websocket_group_chat(websocket: WebSocket, room_code: str):
    room_code = room_code.upper()
    roll      = websocket.query_params.get("roll", "").upper()
    subject   = websocket.query_params.get("subject", "CORE_TRACK").upper()

    if not roll:
        await websocket.close(code=4001); return
    if not faculty_session_manager.is_session_valid(room_code):
        await websocket.close(code=4041); return

    user_data = await db_get_user(roll)
    display_name = user_data["profile"]["name"] if user_data else roll
    is_prof = "FAC_" in roll or "DR_" in roll or "PROF_" in roll or (user_data and user_data.get("role") == "faculty")

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
        "text": f"📌 {'Professor ' if is_prof else ''}{display_name} synchronized to workspace channel.",
        "ts": time.time(),
    }, exclude=websocket)

    await room_manager.broadcast_all(room_code, {"type": "members", "members": room_manager.member_list(room_code)})

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
                q_msg = {"type":"agent_question","roll":roll,"name":display_name, "text":f"/agent {query}","ts":time.time()}
                await db_save_group_message(room_code, q_msg)
                await room_manager.broadcast_all(room_code, q_msg)
                await room_manager.broadcast_all(room_code, {"type":"agent_typing","text":"EduAgent is compiling optimal cluster vectors…"})

                if clients is None:
                    reply = {"type":"agent_reply","text":"⚠️ API orchestration channels currently detached.","track":"error","ts":time.time()}
                else:
                    try:
                        mock = {"name": display_name, "learning_style": "mixed", "metrics": {"python_marks": 85}}
                        answer, meta = await run_academic_pipeline(clients, mock, subject, query)
                        reply = {"type":"agent_reply","text":answer,"track":"academic",
                                 "latency_ms":round(sum(meta["blast_timings_ms"].values()),1),
                                 "ts":time.time()}
                    except Exception as exc:
                        reply = {"type":"agent_reply","text":f"⚠️ Orchestration tracking mismatch exception: {exc}","track":"error","ts":time.time()}

                await db_save_group_message(room_code, reply)
                await room_manager.broadcast_all(room_code, reply)

    except WebSocketDisconnect:
        room_manager.disconnect(room_code, member)
        active_campus_sockets.pop(roll, None)
        await room_manager.broadcast(room_code, {"type":"system","text":f"👋 {display_name} exited channel context.","ts":time.time()})
        await room_manager.broadcast_all(room_code, {"type":"members","members":room_manager.member_list(room_code)})
