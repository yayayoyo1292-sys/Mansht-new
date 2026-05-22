"""
Binary political classifier.

For AI-classified categories (UAE, Saudi, Egypt, Gulf, Arab, World):
  Output: 0 = عام (General)
          1 = سياسة (Politics)

For non-AI categories (Sports, Arts, Economy, etc.):
  Classification is SKIPPED — the category is determined directly from
  the scrape source URL (see config/settings.py → CATEGORIES).
"""
from __future__ import annotations

import os
import re
import unicodedata
from typing import Optional

import joblib

# ─────────────────────────────────────────────────────────────────────────────
# Model loading
# ─────────────────────────────────────────────────────────────────────────────

_BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
_MODEL_PATH = os.path.join(_BASE_DIR, "model.pkl")
_VEC_PATH   = os.path.join(_BASE_DIR, "vectorizer.pkl")

_model      = joblib.load(_MODEL_PATH)
_vectorizer = joblib.load(_VEC_PATH)

# Binary label map  (model may produce Arabic labels from old dataset)
# Normalise everything to int 0 or 1
_LABEL_MAP: dict = {
    # Arabic labels that may exist in old model
    "سياسة":    1,
    "سياسه":    1,
    "politics": 1,
    1:           1,
    "عام":      0,
    "اجتماعية": 0,
    "اجتماعيه": 0,
    "رياضة":    0,
    "فن":       0,
    "general":  0,
    0:           0,
}

CONFIDENCE_THRESHOLD: float = 0.40

# ─────────────────────────────────────────────────────────────────────────────
# Text normalisation (identical to train_model.py — must stay in sync)
# ─────────────────────────────────────────────────────────────────────────────

def normalize_arabic(text: str) -> str:
    text = str(text)
    text = re.sub(r"[إأآا]", "ا", text)
    text = re.sub(r"ى",      "ي", text)
    text = re.sub(r"ؤ",      "و", text)
    text = re.sub(r"ئ",      "ي", text)
    text = re.sub(r"ة",      "ه", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\d+",    " ", text)
    text = re.sub(r"\s+",    " ", text)
    return text.strip()


# ─────────────────────────────────────────────────────────────────────────────
# Classification
# ─────────────────────────────────────────────────────────────────────────────

def classify_political(
    title: str,
    content: Optional[str] = None,
) -> tuple[int, float]:
    """
    Classify a news article as political (1) or general (0).

    Returns
    -------
    (label: int, confidence: float)
        label      = 1  →  سياسة  (Politics)
        label      = 0  →  عام    (General)
        confidence = model probability for predicted class
    """
    text = normalize_arabic(title)
    if content:
        text += " " + normalize_arabic(content)

    try:
        X      = _vectorizer.transform([text])
        probs  = _model.predict_proba(X)[0]
        classes = _model.classes_

        best_idx    = int(probs.argmax())
        raw_label   = classes[best_idx]
        confidence  = float(probs[best_idx])

        # Normalise raw label to 0 or 1
        label = _LABEL_MAP.get(raw_label, 0)

        if confidence < CONFIDENCE_THRESHOLD:
            # Below threshold → default to general
            return 0, confidence

        return label, confidence

    except Exception as exc:
        import logging
        logging.getLogger(__name__).error(f"❌ Classifier error: {exc}")
        return 0, 0.0


def get_template_key(label: int) -> str:
    """Map binary label to template key string."""
    from config.settings import AI_TEMPLATE_MAP
    return AI_TEMPLATE_MAP.get(label, "عام")


# ─────────────────────────────────────────────────────────────────────────────
# Legacy shim — kept for backward compatibility with any remaining callers
# ─────────────────────────────────────────────────────────────────────────────

def classify_news(
    title: str,
    content: Optional[str] = None,
) -> tuple[Optional[str], float]:
    """
    DEPRECATED — use classify_political() for new code.
    Returns Arabic category string + confidence, for backward compatibility.
    """
    label, conf = classify_political(title, content)
    arabic = "سياسة" if label == 1 else "عام"
    return arabic, conf
