"""
Binary political classifier — training pipeline.

يتدرب على ملف CSV جاهز (political_vs_others_dataset.csv).
إذا موجود في نفس الفولدر، يُستخدم وحده.
إذا موجود DB → يُدمج معه للتحسين المستمر.

Columns expected in CSV:
    clean_text  : النص المنظّف مسبقاً
    label       : 0 = عام   |   1 = سياسة
"""
from __future__ import annotations

import os
import sys
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import joblib
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, accuracy_score
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────

_BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH       = os.path.join(_BASE_DIR, "model.pkl")
VEC_PATH         = os.path.join(_BASE_DIR, "vectorizer.pkl")
DATASET_CSV_PATH = os.path.join(_BASE_DIR, "political_vs_others_dataset.csv")

# ─────────────────────────────────────────────────────────────────────────────
# Arabic stopwords — confirmed effective for political classification
# ─────────────────────────────────────────────────────────────────────────────

ARABIC_STOPWORDS = {
    "في", "من", "الى", "إلى", "على", "عن", "مع", "هذا", "هذه",
    "هو", "هي", "هم", "هن", "كان", "تم", "ما", "لا", "لم", "لن",
    "كل", "قد", "وقد", "و", "او", "أو", "ان", "إن", "أن", "بأن",
    "التي", "الذي", "الذين", "اللذان", "ذلك", "هناك", "حيث", "إذ",
    "بعد", "قبل", "حتى", "إلا", "فقط", "أيضا", "أيضاً", "بين",
    "عند", "منذ", "خلال", "وفق", "وفقا", "وفقاً", "نحو", "حول",
    "بشأن", "ضد", "تجاه", "إثر", "عقب", "إزاء",
}

# ─────────────────────────────────────────────────────────────────────────────
# Text normalisation — يجب أن تكون مطابقة تماماً لـ ai.py
# ─────────────────────────────────────────────────────────────────────────────

def normalize_arabic(text: str) -> str:
    text = str(text)
    text = re.sub(r"[إأآا]", "ا", text)
    text = re.sub(r"ى",       "ي", text)
    text = re.sub(r"ؤ",       "و", text)
    text = re.sub(r"ئ",       "ي", text)
    text = re.sub(r"ة",       "ه", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\d+",     " ", text)
    text = re.sub(r"\s+",     " ", text)
    return text.strip()


def _label_to_int(val) -> int:
    """أي شكل للـ label → 0 أو 1."""
    if isinstance(val, int):
        return 1 if val == 1 else 0
    s = str(val).strip()
    if s in ("1", "سياسة", "سياسه", "politics"):
        return 1
    return 0

# ─────────────────────────────────────────────────────────────────────────────
# Data loaders
# ─────────────────────────────────────────────────────────────────────────────

def load_csv() -> pd.DataFrame:
    """
    يقرأ ملف الـ CSV.
    Columns: clean_text, label
    الـ clean_text موجود مسبقاً في الـ CSV — لا يحتاج re-cleaning.
    نعمل له normalize_arabic فقط لضمان توحيد الحروف.
    """
    if not os.path.exists(DATASET_CSV_PATH):
        raise FileNotFoundError(
            f"❌ ملف الداتا سيت غير موجود:\n   {DATASET_CSV_PATH}\n"
            "   شغّل سكريبت إنشاء الداتا سيت أولاً."
        )

    df = pd.read_csv(DATASET_CSV_PATH, encoding="utf-8")

    # تأكد من الأعمدة المطلوبة
    if "clean_text" not in df.columns or "label" not in df.columns:
        raise ValueError(
            "❌ الـ CSV لازم يحتوي على عمودين: 'clean_text' و 'label'\n"
            f"   الأعمدة الموجودة: {list(df.columns)}"
        )

    # تنظيف
    df = df.dropna(subset=["clean_text", "label"])
    df["text"]  = df["clean_text"].astype(str).apply(normalize_arabic)
    df["label"] = df["label"].apply(_label_to_int)

    # إزالة النصوص الفارغة أو القصيرة جداً بعد التطبيع
    df = df[df["text"].str.split().str.len() >= 3]
    df = df.drop_duplicates(subset=["text"])

    print(f"📁 CSV محمّل: {len(df):,} مثال فريد")
    return df[["text", "label"]]


def load_from_db(min_confidence: float = 0.60) -> pd.DataFrame:
    """
    يجلب بيانات التدريب المؤكدة من الـ DB.
    اختياري — يُستخدم فقط لتحسين الموديل مع الوقت.
    """
    try:
        from DB.db import get_conn
        conn = get_conn()
        cur  = conn.cursor()

        cur.execute("""
            SELECT title, label FROM confirmed_training
            WHERE label IN (0, 1) AND title IS NOT NULL
        """)
        confirmed = cur.fetchall()

        cur.execute("""
            SELECT title, category FROM news
            WHERE confidence >= %s
              AND category IN ('سياسة', 'عام')
              AND source_category IN ('uae','saudi','egypt','gulf','arab','world')
              AND title IS NOT NULL
        """, (min_confidence,))
        news_rows = cur.fetchall()
        conn.close()

        rows = []
        seen: set[str] = set()

        for title, label in confirmed:
            norm = normalize_arabic(str(title))
            if norm and norm not in seen:
                seen.add(norm)
                rows.append({"text": norm, "label": _label_to_int(label)})

        for title, category in news_rows:
            norm = normalize_arabic(str(title))
            if norm and norm not in seen:
                seen.add(norm)
                rows.append({"text": norm, "label": _label_to_int(category)})

        if rows:
            df_db = pd.DataFrame(rows)
            print(f"🗄️  DB: {len(confirmed):,} مؤكد + {len(news_rows):,} أخبار → {len(df_db):,} فريد")
            return df_db

    except Exception as exc:
        print(f"⚠️  DB غير متاح (يُتجاهل): {exc}")

    return pd.DataFrame(columns=["text", "label"])

# ─────────────────────────────────────────────────────────────────────────────
# Vectorizer config — مُحسَّن للعربية السياسية
# ─────────────────────────────────────────────────────────────────────────────

def build_vectorizer(n_samples: int) -> TfidfVectorizer:
    """
    نختار max_features بناءً على حجم الداتا لتفادي الـ overfitting.
      < 500  عينة  → 1000 feature
      < 2000 عينة  → 2000 feature
      ≥ 2000 عينة  → 3000 feature
    """
    if n_samples < 500:
        max_f = 1000
    elif n_samples < 2000:
        max_f = 2000
    else:
        max_f = 3000

    return TfidfVectorizer(
        analyzer="word",
        ngram_range=(1, 3),          # unigrams + bigrams + trigrams (مهم جداً للعربية السياسية)
        max_features=max_f,
        min_df=2,                    # يتجاهل الكلمات النادرة جداً
        max_df=0.90,                 # يتجاهل الكلمات الشائعة جداً في الكل
        stop_words=list(ARABIC_STOPWORDS),
        sublinear_tf=True,           # log(TF) بدل TF الخام — أثبت نتائج أفضل للعربية
        strip_accents=None,          # لا نحذف التشكيل — قد يكون مؤشر دلالي
    )

# ─────────────────────────────────────────────────────────────────────────────
# Main training pipeline
# ─────────────────────────────────────────────────────────────────────────────

def train_pipeline() -> None:
    print("\n" + "=" * 60)
    print("  BINARY POLITICAL CLASSIFIER — TRAINING PIPELINE")
    print("=" * 60)

    # ── 1. Load data ──────────────────────────────────────────────
    df_csv = load_csv()
    df_db  = load_from_db(min_confidence=0.60)

    if not df_db.empty:
        df = pd.concat([df_csv, df_db], ignore_index=True)
        df = df.drop_duplicates(subset=["text"])
        print(f"🔀 دمج CSV + DB → {len(df):,} مثال إجمالي")
    else:
        df = df_csv

    # ── 2. Balance check ──────────────────────────────────────────
    print(f"\n📊 توزيع التصنيفات:")
    vc = df["label"].value_counts()
    count_general  = vc.get(0, 0)
    count_politics = vc.get(1, 0)
    print(f"   0 (عام)    : {count_general:,}")
    print(f"   1 (سياسة) : {count_politics:,}")

    if df["label"].nunique() < 2:
        raise ValueError("❌ يجب وجود صنفين (0 و 1) على الأقل. أضف بيانات أكثر.")

    if min(count_general, count_politics) < 20:
        print("⚠️  تحذير: الصنف الأقل يحتوي على أقل من 20 مثال — الدقة ستكون محدودة.")

    # ── 3. Train / test split ─────────────────────────────────────
    X_train_txt, X_test_txt, y_train, y_test = train_test_split(
        df["text"], df["label"],
        test_size=0.20,
        random_state=42,
        stratify=df["label"],
    )

    # ── 4. Vectorize ──────────────────────────────────────────────
    vectorizer = build_vectorizer(len(df))
    X_train    = vectorizer.fit_transform(X_train_txt)
    X_test     = vectorizer.transform(X_test_txt)
    print(f"\n🔤 Vocabulary size : {len(vectorizer.vocabulary_):,} feature")
    print(f"   Train samples   : {X_train.shape[0]:,}")
    print(f"   Test  samples   : {X_test.shape[0]:,}")

    # ── 5. Model ──────────────────────────────────────────────────
    # C=1.0 أفضل من 0.5 مع داتا سيت متوازنة وواضحة المعالم
    model = LogisticRegression(
        max_iter=5000,
        C=1.0,
        class_weight="balanced",
        solver="saga",
        tol=1e-4,
        random_state=42,
        n_jobs=-1,
    )

    print("\n🧠 جاري التدريب ...")
    model.fit(X_train, y_train)

    # ── 6. Evaluation ─────────────────────────────────────────────
    predictions   = model.predict(X_test)
    train_acc     = model.score(X_train, y_train)
    test_acc      = accuracy_score(y_test, predictions)

    print(f"\n{'='*40}")
    print(f"  دقة التدريب  : {train_acc:.4f}")
    print(f"  دقة الاختبار : {test_acc:.4f}")

    # Overfitting warning
    gap = train_acc - test_acc
    if gap > 0.12:
        print(f"  ⚠️  فجوة كبيرة ({gap:.3f}) — احتمال Overfitting. زد الداتا أو قلّل max_features.")

    # Cross-validation (5-fold) — أكثر موثوقية من split واحد
    print(f"\n🔄 Cross-Validation (5-fold) ...")
    cv_scores = cross_val_score(
        model, vectorizer.transform(df["text"]), df["label"],
        cv=StratifiedKFold(n_splits=5, shuffle=True, random_state=42),
        scoring="f1_weighted",
        n_jobs=-1,
    )
    print(f"  F1 weighted (CV) : {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

    print(f"\n{'='*40}")
    print("تقرير التصنيف التفصيلي:")
    print(classification_report(
        y_test, predictions,
        target_names=["عام (0)", "سياسة (1)"],
        digits=4,
    ))

    # ── 7. Save ───────────────────────────────────────────────────
    joblib.dump(model,      MODEL_PATH)
    joblib.dump(vectorizer, VEC_PATH)
    print(f"💾 الموديل محفوظ    : {MODEL_PATH}")
    print(f"💾 Vectorizer محفوظ : {VEC_PATH}")
    print("\n✅ تم التدريب بنجاح\n")


if __name__ == "__main__":
    train_pipeline()
