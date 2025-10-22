# absa_gemini_lc_reviews_table.py
import os, json, time, hashlib, sqlite3
from typing import List, Optional
from pydantic import BaseModel, field_validator, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

from langchain_core.prompts import ChatPromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI

if not os.environ.get("GOOGLE_API_KEY"):
  os.environ["GOOGLE_API_KEY"] = "AIzaSyCMImjFMxNmcgPdIuAK9UkAkDPSDN_8YMo"

DB_PATH = "C:\\Projects\\NLP\\ASBA\\Scrapper\\Scrappers\\reviews.db"                        # <-- kendi .sqlite yolun
MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
BATCH_SIZE = 64
SLEEP_BETWEEN_CALLS = 0.15
MAX_ROWS = 10000

# ---------- Pydantic output schema ----------
class AspectItem(BaseModel):
    aspect: str
    category: str
    sentiment: str  # positive|negative|neutral|mixed
    opinion_terms: str
    start_idx: Optional[int] = None
    end_idx: Optional[int] = None
    confidence: Optional[float] = None

    @field_validator("sentiment")
    @classmethod
    def check_sentiment(cls, v):
        allowed = {"positive", "negative", "neutral", "mixed"}
        if v not in allowed:
            raise ValueError(f"sentiment must be one of {allowed}")
        return v

class ABSAResponse(BaseModel):
    aspects: List[AspectItem]

# ---------- LLM ----------
llm = ChatGoogleGenerativeAI(
    model=MODEL_NAME,
    temperature=0.2,
)

SYSTEM_MSG = (
    "Türkçe ürün yorumlarından aspect ve duygu (sentiment) çıkaran bir yardımcı olarak çalış.\n"
    "Çıktı kesinlikle geçerli JSON yapısında olmalı (yapısal çıktı). Açıklama ekleme."
)

USER_TEMPLATE = """Aşağıdaki ürün yorumu için aspect-based sentiment çıkar:
- Yalnızca yoruma dayan.
- Birden çok aspect dönebilirsin.
- "sentiment": positive|negative|neutral|mixed.
- "category": serbest metin (örn. donanım/ekran, performans, fiyat, kargo).
- "opinion_terms": metinden aynen ilgili ifade(ler).
- Mümkünse "start_idx" ve "end_idx" (Unicode index).
- "confidence": 0..1.

Yorum:
<<<
{review_text}
>>>
"""

prompt = ChatPromptTemplate.from_messages([
    ("system", SYSTEM_MSG),
    ("user", USER_TEMPLATE),
])

structured_llm = llm.with_structured_output(ABSAResponse)
chain = prompt | structured_llm

# ---------- helpers ----------
def prompt_hash(system_msg: str, user_filled: str) -> str:
    h = hashlib.sha256()
    h.update(system_msg.encode("utf-8"))
    h.update(user_filled.encode("utf-8"))
    return h.hexdigest()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def call_chain(review_text: str) -> ABSAResponse:
    return chain.invoke({"review_text": review_text.strip()})

def upsert_results(conn, review_id: int, phash: str, parsed: ABSAResponse):
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO absa_raw (review_id, model_name, prompt_hash, response_json) VALUES (?, ?, ?, ?)",
        (review_id, MODEL_NAME, phash, json.dumps(parsed.model_dump(), ensure_ascii=False))
    )
    for item in parsed.aspects:
        cur.execute(
            """INSERT OR IGNORE INTO absa_aspects 
               (review_id, aspect, category, sentiment, opinion_terms, start_idx, end_idx, confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                review_id,
                item.aspect.strip(),
                item.category.strip(),
                item.sentiment.strip(),
                item.opinion_terms.strip(),
                item.start_idx if item.start_idx is not None else None,
                item.end_idx if item.end_idx is not None else None,
                float(item.confidence) if item.confidence is not None else None
            )
        )
    conn.commit()

def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # --- tables for outputs
    cur.execute("""CREATE TABLE IF NOT EXISTS absa_raw (
      review_id INTEGER PRIMARY KEY,
      model_name TEXT,
      prompt_hash TEXT,
      response_json TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS absa_aspects (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      review_id INTEGER,
      aspect TEXT,
      category TEXT,
      sentiment TEXT,
      opinion_terms TEXT,
      start_idx INTEGER,
      end_idx INTEGER,
      confidence REAL,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(review_id, aspect, category, sentiment, opinion_terms)
    )""")
    # faydalı indexler
    cur.execute("CREATE INDEX IF NOT EXISTS idx_absa_aspects_review ON absa_aspects(review_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_absa_raw_hash      ON absa_raw(prompt_hash)")
    conn.commit()

    # --- READ from your reviews table ---
    # Şeman: reviews(id, product_id, review_hash, review_text, ...)
    # Henüz işlenmemişleri çekiyoruz (absa_raw'da kaydı olmayanlar)
    cur.execute("""
        SELECT r.id, r.review_text
        FROM reviews r
        LEFT JOIN absa_raw a ON a.review_id = r.id
        WHERE a.review_id IS NULL
          AND r.review_text IS NOT NULL
          AND TRIM(r.review_text) <> ''
        LIMIT ?
    """, (MAX_ROWS,))
    rows = cur.fetchall()

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        for review_id, review_text in batch:
            text = (review_text or "").strip()
            if not text:
                continue

            user_filled = USER_TEMPLATE.format(review_text=text)
            phash = prompt_hash(SYSTEM_MSG, user_filled)

            # cache: aynı prompt_hash ile bu review zaten işlendiyse atla
            cur.execute("SELECT 1 FROM absa_raw WHERE review_id=? AND prompt_hash=?", (review_id, phash))
            if cur.fetchone():
                continue

            try:
                parsed: ABSAResponse = call_chain(text)
            except Exception as e:
                print(f"[LLM ERROR] id={review_id}: {e}")
                continue

            try:
                ABSAResponse.model_validate(parsed.model_dump())
            except ValidationError as ve:
                print(f"[VALIDATION FAIL] id={review_id}: {ve}")
                continue

            upsert_results(conn, review_id, phash, parsed)
            time.sleep(SLEEP_BETWEEN_CALLS)

    conn.close()

if __name__ == "__main__":
    main()
