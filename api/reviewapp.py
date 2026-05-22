from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from DB.db import get_conn
import os
import uvicorn

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "https://mansht-final.vercel.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


LOCK_TIMEOUT_MINUTES = 10


# =========================
# REQUEST MODEL
# =========================

class ReviewRequest(BaseModel):
    id: int
    category: str
    reviewer: str



@app.get("/news/review")
def get_news(reviewer: str):

    conn = get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            WITH next_article AS (
                SELECT id
                FROM news
                WHERE confidence < 0.65
                AND reviewed = FALSE
                AND (
                    locked_by IS NULL
                    OR locked_at < NOW() - INTERVAL '10 minutes'
                )
                ORDER BY created_at DESC
                LIMIT 1
            )
            UPDATE news n
            SET locked_by = %s,
                locked_at = NOW()
            FROM next_article
            WHERE n.id = next_article.id
            RETURNING n.id, n.title, n.category, n.confidence
        """, (reviewer,))

        row = cursor.fetchone()
        conn.commit()

        if not row:
            return {"message": "No articles left"}

        return {
            "id": row[0],
            "title": row[1],
            "predicted": row[2],
            "confidence": row[3]
        }

    finally:
        cursor.close()
        conn.close()


@app.post("/news/review")
def review_news(data: ReviewRequest):

    conn = get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            UPDATE news
            SET category = %s,
                reviewed = TRUE,
                locked_by = NULL,
                locked_at = NULL
            WHERE id = %s
        """, (
            data.category,
            data.id
        ))

        conn.commit()

        return {
            "success": True,
            "message": "Review saved"
        }

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("reviewapp:app", host="0.0.0.0", port=port)
