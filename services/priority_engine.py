"""
services/priority_engine.py — UAE Newsroom Priority Scoring Engine

COMPLETE REWRITE — replaces the old flat keyword list with a professional
weighted scoring system designed for a real Arabic-language newsroom.

ARCHITECTURE
────────────────────────────────────────────────────────────────────────────
Score = keyword_score + aging_score + ai_score

keyword_score  → weighted match against the NEW priority taxonomy below
aging_score    → gentle linear bonus as articles sit in the queue
ai_score       → bonus for detecting named high-value entities

Instagram threshold (PRIORITY_THRESHOLD_INSTAGRAM = 9):
  Only articles that score ≥ 9 go to Instagram.
  All others go to Facebook + Twitter.

KEYWORD TAXONOMY (5 tiers)
────────────────────────────────────────────────────────────────────────────
Tier 1 — National Leadership (score 20)
  Presidential, ruler, VP, crown prince statements and decrees.
  These are the most time-sensitive and highest-engagement articles.

Tier 2 — Senior Leadership & Strategic (score 15)
  Ministers, emirate rulers, senior federal officials.
  High public interest, strong follower engagement.

Tier 3 — National Identity & Institutions (score 11)
  Emirates-brand topics, national events, major federal bodies.
  Consistent Instagram engagement even without a named leader.

Tier 4 — Prestige / Flagship Topics (score 8)
  UAE sports teams, airlines, strategic sectors, international events.
  Good Instagram content but below Instagram-only threshold (9).
  These go to Facebook + Twitter with the normal flow.

Tier 5 — Generic / Contextual (score 5)
  Cities, broad topics, common entities.
  Provide a scoring bump but never trigger Instagram alone.
  Prevents false negatives for articles combining T5 + T4/3 keywords.

FALSE-POSITIVE GUARD
────────────────────────────────────────────────────────────────────────────
Generic single-word triggers (وزير, حاكم, أول, أكبر, دبي, أبوظبي) are in
Tier 5 so they can't trigger Instagram by themselves.
Only their combination with higher-tier matches can push a score over 9.

NORMALIZATION
────────────────────────────────────────────────────────────────────────────
All text is NFC-normalized and diacritics (تشكيل) are stripped before
matching.  This handles the real-world variability in Arabic text scraped
from news sites.

LOGGING
────────────────────────────────────────────────────────────────────────────
Every scoring call logs which keywords matched, their individual scores,
and the final decision.  This makes it easy to tune weights in production.
"""
from __future__ import annotations

import re
import time
import unicodedata
from typing import Optional

from config.settings import AGING_MULTIPLIER
from utils.logger import logger


# ─────────────────────────────────────────────────────────────────────────────
# KEYWORD TAXONOMY
# Each tier is a tuple of (score, [keyword_list]).
# ─────────────────────────────────────────────────────────────────────────────

_KEYWORD_TIERS: list[tuple[int, list[str]]] = [

    # ── TIER 1: National Leadership (score 20) ────────────────────────────
    # Presidential / ruler-class figures.  Single match → Instagram.
    (20, [
        "رئيس الدولة",
        "محمد بن زايد",
        "محمد بن زايد آل نهيان",
        "محمد بن راشد",
        "محمد بن راشد آل مكتوم",
        "منصور بن زايد",
        "منصور بن زايد آل نهيان",
        "الشيخة فاطمة",
        "أم الإمارات",
        "الشيخ زايد",
    ]),

    # ── TIER 2: Senior Leadership & Strategic (score 15) ─────────────────
    # Emirate rulers, crown princes, senior cabinet figures.
    # Single match → Instagram.
    (15, [
        "سلطان القاسمي",
        "حمد الشرقي",
        "حميد بن راشد",
        "سعود بن صقر",
        "سعود المعلا",
        "خالد بن محمد بن زايد",
        "حمدان بن محمد",
        "حمدان بن زايد",
        "سيف بن زايد",
        "عبدالله بن زايد",
        "طحنون بن زايد",
        "ذياب بن محمد بن زايد",
        "هزاع بن زايد",
        "نهيان بن زايد",
        "نهيان بن مبارك",
        "زايد بن محمد بن زايد",
        "خالد بن زايد",
        "شخبوط بن نهيان",
        "مكتوم بن محمد",
        "أحمد بن محمد",
        "منصور بن محمد",
        "عمار بن حميد",
        "محمد بن سعود",
        "محمد الشرقي",
        "أنور قرقاش",
        "ثاني الزيودي",
        "لطيفة بنت محمد",
        "حمدان بن محمد زايد",
        "صقر غباش",
        "عبدالله آل حامد",
        "سلطان النيادي",
    ]),

    # ── TIER 3: National Identity & Institutions (score 11) ──────────────
    # UAE-brand news, national bodies, strategic agencies.
    # Single match → Instagram.
    (11, [
        "الإمارات",
        "مجلس الوزراء",
        "حكام الإمارات",
        "حكومة الإمارات",
        "المجلس التنفيذي",
        "تنفيذي أبوظبي",
        "الوطني الاتحادي",
        "اليوم الوطني",
        "يوم الشهيد",
        "المرأة الإماراتية",
        "التوازن بين الجنسين",
        "التنمية الأسرية",
        "الاتحاد النسائي",
        "حكماء المسلمين",
        "وساطة إماراتية",
        "الدفاعات الجوية الإماراتية",
        "الفارس الشهم",
        "المساعدات الإنسانية",
        "صندوق أبوظبي",
        "براكة",
    ]),

    # ── TIER 4: Prestige / Flagship Topics (score 8) ─────────────────────
    # Below the Instagram threshold on their own, but important enough to
    # combine with Tier 5 and push an article over.
    (8, [
        "شرطة أبوظبي",
        "شرطة دبي",
        "شرطة الشارقة",
        "صحة أبوظبي",
        "صحة دبي",
        "بيئة أبوظبي",
        "بلدية أبوظبي",
        "النقل الذكي",
        "الاتحاد للطيران",
        "طيران الإمارات",
        "موانئ أبوظبي",
        "أبوظبي العالمي",
        "القطاع العقاري",
        "النفط",
        "الذهب",
        "الدولار",
        "الذكاء الاصطناعي",
        "الأمن السيبراني",
        "الفضاء",
        "سماء الإمارات",
        "منتخب الإمارات",
        "منتخبنا الوطني",
        "كأس الإمارات",
        "دوري",
        "فروسية",
        "هجن",
        "خيول",
        "سباق زايد الخيري",
        "أبوظبي للرياضات البحرية",
        "فرسان الإمارات",
        "وزير الرياضة",
        "بطولة",
        "يدين",
        "تدين",
        "اليوم الدولي",
        "اليوم العالمي",
        "متحف",
        "متاحف",
    ]),

    # ── TIER 5: Generic / Contextual (score 5) ────────────────────────────
    # Cannot trigger Instagram alone.  Provide a bump when combined.
    # E.g. "دبي" + "وزير" + "بطولة" = 5+5+8 = 18 → Instagram
    # But "دبي" alone = 5 → Facebook + Twitter only
    (5, [
        "أبوظبي",
        "دبي",
        "الشارقة",
        "الجزيرة",
        "العين",
        "ولي عهد",
        "حاكم",
        "نائب حاكم",
        "وزير",
        "الأبيض",
        "أول",
        "الأولى",
        "أكبر",
    ]),
]

# ─────────────────────────────────────────────────────────────────────────────
# Compile tier data for fast lookup
# ─────────────────────────────────────────────────────────────────────────────

# keyword → (tier_score, tier_number)  for deduplication + logging
_KW_MAP: dict[str, tuple[int, int]] = {}
for tier_idx, (tier_score, keywords) in enumerate(_KEYWORD_TIERS, start=1):
    for kw in keywords:
        if kw not in _KW_MAP:   # first/highest tier wins
            _KW_MAP[kw] = (tier_score, tier_idx)

# Important named entities for ai_score bonus
_NAMED_ENTITIES: list[str] = [
    "محمد بن زايد",
    "محمد بن راشد",
    "منصور بن زايد",
    "ولي عهد",
    "الإمارات",
    "حاكم",
    "رئيس الدولة",
    "مجلس الوزراء",
]


# ─────────────────────────────────────────────────────────────────────────────
# Text normalization
# ─────────────────────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    """
    NFC-normalize, strip diacritics (تشكيل), and fold to lowercase.
    Handles real-world Arabic text variance from scraper output.
    """
    text = unicodedata.normalize("NFC", str(text or ""))
    # Strip Arabic diacritics (U+064B – U+065F, U+0670)
    text = re.sub(r"[\u064B-\u065F\u0670]", "", text)
    return text.strip()


# ─────────────────────────────────────────────────────────────────────────────
# Core scoring functions
# ─────────────────────────────────────────────────────────────────────────────

def calculate_priority_score(
    title: str,
    content: str = "",
) -> int:
    """
    Return the HIGHEST single-keyword score found in title+content.
    This is the canonical priority_score stored in news_queue.

    Uses max (not sum) so one ultra-high match doesn't get diluted
    by many low-tier matches — the most important article always wins.
    """
    text    = _normalize(f"{title} {content}")
    highest = 0
    for kw, (score, _tier) in _KW_MAP.items():
        if _normalize(kw) in text:
            if score > highest:
                highest = score
    return highest


def calculate_keyword_score(
    title: str,
    content: str = "",
) -> int:
    """
    Return the CUMULATIVE keyword score (sum of all matches).
    Used for final_score calculation — rewards articles with many matches.

    Example: "محمد بن زايد يفتتح مشروع الذكاء الاصطناعي في أبوظبي"
      محمد بن زايد = 20
      الذكاء الاصطناعي = 8
      أبوظبي = 5
      total = 33
    """
    text  = _normalize(f"{title} {content}")
    score = 0
    seen  : set[str] = set()   # deduplicate — same keyword counted once
    for kw, (kw_score, _tier) in _KW_MAP.items():
        nkw = _normalize(kw)
        if nkw not in seen and nkw in text:
            score += kw_score
            seen.add(nkw)
    return score


def explain_priority(
    title: str,
    content: str = "",
) -> dict:
    """
    Return a full explanation dict for logging and debugging.

    Returns:
      {
        "priority_score": int,          # max single match
        "keyword_score":  int,          # cumulative sum
        "matched": [                    # list of matched keywords
          {"keyword": str, "score": int, "tier": int},
          ...
        ],
        "instagram_eligible": bool,
        "reason": str,                  # human-readable explanation
      }
    """
    from config.settings import PRIORITY_THRESHOLD_INSTAGRAM

    text    = _normalize(f"{title} {content}")
    matched = []
    seen    : set[str] = set()

    for kw, (kw_score, tier) in _KW_MAP.items():
        nkw = _normalize(kw)
        if nkw not in seen and nkw in text:
            matched.append({"keyword": kw, "score": kw_score, "tier": tier})
            seen.add(nkw)

    # Sort by score descending for readable output
    matched.sort(key=lambda m: (-m["score"], m["tier"]))

    priority_score = max((m["score"] for m in matched), default=0)
    keyword_score  = sum(m["score"] for m in matched)
    eligible       = priority_score >= PRIORITY_THRESHOLD_INSTAGRAM

    if not matched:
        reason = "No priority keywords matched — normal routing (Facebook + Twitter)"
    elif eligible:
        top_kw = matched[0]
        reason = (
            f"Matched '{top_kw['keyword']}' (Tier {top_kw['tier']}, "
            f"score={top_kw['score']}) → Instagram-eligible"
        )
    else:
        top_kw = matched[0]
        reason = (
            f"Best match '{top_kw['keyword']}' (Tier {top_kw['tier']}, "
            f"score={top_kw['score']}) below Instagram threshold "
            f"({PRIORITY_THRESHOLD_INSTAGRAM}) → Facebook + Twitter"
        )

    return {
        "priority_score":     priority_score,
        "keyword_score":      keyword_score,
        "matched":            matched,
        "instagram_eligible": eligible,
        "reason":             reason,
    }


def log_priority_decision(title: str, content: str = "") -> int:
    """
    Score an article and emit a structured log entry.
    Returns priority_score for use in publish routing.
    Called once per article in process_article().
    """
    from config.settings import PRIORITY_THRESHOLD_INSTAGRAM

    exp = explain_priority(title, content)
    ps  = exp["priority_score"]

    if exp["matched"]:
        kw_list = ", ".join(
            f"'{m['keyword']}'={m['score']}" for m in exp["matched"][:5]
        )
        logger.info(
            f"🏷️  Priority | score={ps} | "
            f"{'🔴 INSTAGRAM' if exp['instagram_eligible'] else '🔵 FB+TW'} | "
            f"matched=[{kw_list}] | {title[:70]}"
        )
    else:
        logger.info(f"🏷️  Priority | score=0 | 🔵 FB+TW | no match | {title[:70]}")

    if exp["instagram_eligible"]:
        logger.info(f"   ↳ Reason: {exp['reason']}")

    return ps


# ─────────────────────────────────────────────────────────────────────────────
# Aging and AI score
# ─────────────────────────────────────────────────────────────────────────────

def calculate_aging_bonus(created_at: float, now: Optional[float] = None) -> float:
    """
    Gently increase score as an article sits in the queue.
    AGING_MULTIPLIER=0.12 → 60 min old article gets +7.2 bonus.
    Keeps old articles from starving but doesn't overwhelm keyword scores.
    """
    if now is None:
        now = time.time()
    age_minutes = max(0.0, (now - created_at) / 60)
    return age_minutes * AGING_MULTIPLIER


def calculate_ai_score(title: str, content: str = "") -> int:
    """
    Bonus for detecting high-value named entities.
    Rewards articles that mention multiple important figures/topics.
    """
    text  = _normalize(f"{title} {content}")
    score = 0
    for entity in _NAMED_ENTITIES:
        if _normalize(entity) in text:
            score += 3
    if len(text) > 500:
        score += 1   # content-rich article gets small bonus
    return score


def calculate_final_score(
    title: str,
    content: str,
    created_at: float,
) -> dict:
    """
    Compute all score components.  Used by QueueManager.add_or_update_queue_item().
    """
    keyword_score = calculate_keyword_score(title, content)
    aging_score   = calculate_aging_bonus(created_at)
    ai_score      = calculate_ai_score(title, content)
    final_score   = keyword_score + aging_score + ai_score

    return {
        "keyword_score": keyword_score,
        "aging_score":   aging_score,
        "ai_score":      ai_score,
        "final_score":   final_score,
    }
