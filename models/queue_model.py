from dataclasses import dataclass


@dataclass
class QueueItem:
    id: int
    title: str
    url: str
    content: str
    created_at: float
    keyword_score: float
    aging_score: float
    ai_score: float
    final_score: float
    publish_status: str = "pending"