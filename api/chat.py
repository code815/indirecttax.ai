from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import os
import httpx

from api.db import conn

router = APIRouter()

FLOW_URL = os.getenv("FLOWISE_CHAT_URL")  # e.g. http://localhost:3000/api/v1/prediction/<flow-id>
FLOW_AUTH = os.getenv("FLOWISE_API_KEY")  # optional

class ChatRequest(BaseModel):
    message: str
    state: Optional[str] = None
    topic: Optional[str] = None
    form_id: Optional[str] = None

@router.post("/chat")
async def chat(req: ChatRequest) -> Dict[str, Any]:
    # If Flowise is configured, proxy to it.
    if FLOW_URL:
        headers = {"Content-Type": "application/json"}
        if FLOW_AUTH:
            headers["Authorization"] = f"Bearer {FLOW_AUTH}"
        payload = {
            "message": req.message,
            "overrideConfig": {
                "state": req.state,
                "topic": req.topic,
                "form_id": req.form_id,
            }
        }
        async with httpx.AsyncClient(timeout=90) as client:
            r = await client.post(FLOW_URL, json=payload, headers=headers)
            r.raise_for_status()
            data = r.json()
        return {"output": data.get("text") or data, "source": "flowise"}

    # Fallback stub (no external LLM): return top 3 recent items to ground a human answer.
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            SELECT s.id, s.title, s.topic, d.url
            FROM snapshots s
            JOIN documents d ON d.id=s.document_id
            ORDER BY s.captured_at DESC
            LIMIT 3
        """)
        recs = cur.fetchall()
    suggestions = [{"id": r[0], "title": r[1], "topic": r[2], "url": r[3]} for r in recs]
    return {
        "output": "Chat backend not configured. Set FLOWISE_CHAT_URL to enable real answers.",
        "suggestions": suggestions,
        "source": "stub"
    }
