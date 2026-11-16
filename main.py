
import os, sqlite3, asyncio
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import Optional
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

DB_PATH = os.getenv("DB_PATH", "tracker.db")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
PROVIDER_URL = os.getenv("PROVIDER_URL", "").strip()
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN", "").strip()

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with db() as c:
        c.execute("""
        CREATE TABLE IF NOT EXISTS offers (
            id TEXT PRIMARY KEY,
            asin TEXT,
            title TEXT NOT NULL,
            price REAL,
            currency TEXT DEFAULT 'EGP',
            condition TEXT,
            seller TEXT,
            product_url TEXT,
            image_url TEXT,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_first_seen ON offers(first_seen DESC)")

def normalize_offer(item):
    # Provider contract: stable offer_id is preferred; otherwise ASIN+seller+condition+price.
    asin = str(item.get("asin") or "")
    seller = str(item.get("seller") or "")
    condition = str(item.get("condition") or "Used")
    price = item.get("price")
    stable = item.get("offer_id") or "|".join([asin, seller, condition, str(price)])
    return {
        "id": str(stable),
        "asin": asin,
        "title": str(item.get("title") or "بدون اسم"),
        "price": float(price) if price not in (None, "") else None,
        "currency": str(item.get("currency") or "EGP"),
        "condition": condition,
        "seller": seller,
        "product_url": str(item.get("product_url") or ""),
        "image_url": str(item.get("image_url") or ""),
    }

async def fetch_provider_offers():
    if not PROVIDER_URL:
        return None, "لم يتم ربط مصدر بيانات حتى الآن"
    headers = {"Accept": "application/json"}
    if PROVIDER_TOKEN:
        headers["Authorization"] = f"Bearer {PROVIDER_TOKEN}"
    async with httpx.AsyncClient(timeout=25) as client:
        response = await client.get(PROVIDER_URL, headers=headers)
        response.raise_for_status()
        payload = response.json()
    # Expected response: {"offers": [{offer_id, asin, title, price, currency,
    # condition, seller, product_url, image_url}, ...]}
    offers = payload.get("offers") if isinstance(payload, dict) else None
    if not isinstance(offers, list):
        raise ValueError("مصدر البيانات يجب أن يعيد JSON بالشكل { offers: [...] }")
    return offers, None

def save_offers(items):
    now = datetime.now(timezone.utc).isoformat()
    added = 0
    with db() as c:
        for raw in items:
            x = normalize_offer(raw)
            exists = c.execute("SELECT id FROM offers WHERE id=?", (x["id"],)).fetchone()
            if exists:
                c.execute("""UPDATE offers SET title=?, price=?, currency=?, condition=?,
                    seller=?, product_url=?, image_url=?, last_seen=? WHERE id=?""",
                    (x["title"], x["price"], x["currency"], x["condition"], x["seller"],
                     x["product_url"], x["image_url"], now, x["id"]))
            else:
                c.execute("""INSERT INTO offers
                    (id, asin, title, price, currency, condition, seller, product_url, image_url, first_seen, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (x["id"], x["asin"], x["title"], x["price"], x["currency"], x["condition"],
                     x["seller"], x["product_url"], x["image_url"], now, now))
                added += 1
    return added

async def sync_once():
    try:
        items, note = await fetch_provider_offers()
        if items is None:
            return {"ok": False, "added": 0, "message": note}
        added = save_offers(items)
        return {"ok": True, "added": added, "message": f"تم فحص المصدر؛ {added} عرض جديد"}
    except Exception as e:
        return {"ok": False, "added": 0, "message": f"تعذر تحديث البيانات: {e}"}

async def poller():
    while True:
        await sync_once()
        await asyncio.sleep(max(POLL_SECONDS, 60))

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    task = asyncio.create_task(poller())
    yield
    task.cancel()

app = FastAPI(title="Amazon.eg Used Tracker", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def home():
    return FileResponse("static/index.html")

@app.get("/api/health")
def health():
    return {"status": "running", "poll_seconds": max(POLL_SECONDS, 60),
            "provider_connected": bool(PROVIDER_URL)}

@app.get("/api/offers")
def offers():
    with db() as c:
        rows = c.execute("SELECT * FROM offers ORDER BY first_seen DESC LIMIT 500").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/sync")
async def sync():
    return await sync_once()

@app.post("/api/demo")
def demo():
    # Clearly labeled demo records; not live Amazon listings.
    now = datetime.now(timezone.utc).isoformat()
    examples = [
        {"offer_id":"DEMO-001","asin":"DEMO001","title":"مثال توضيحي — سماعة لاسلكية","price":850,
         "currency":"EGP","condition":"Used - Like New","seller":"Demo seller",
         "product_url":"https://www.amazon.eg/","image_url":""},
        {"offer_id":"DEMO-002","asin":"DEMO002","title":"مثال توضيحي — ماكينة قهوة","price":1250,
         "currency":"EGP","condition":"Used - Very Good","seller":"Demo seller",
         "product_url":"https://www.amazon.eg/","image_url":""}
    ]
    added = save_offers(examples)
    return {"ok": True, "added": added, "message":"أضيفت بيانات تجريبية واضحة وليست عروضًا حقيقية"}
