"""
image/composer.py — Branded post image generator.

═══════════════════════════════════════════════════════════════════════════════
FIXES APPLIED
═══════════════════════════════════════════════════════════════════════════════

ISSUE #1 — News image appears behind the template layer
────────────────────────────────────────────────────────
ROOT CAUSE A — Missing 'سيارات' from hardcoded IF list:
  The original code decided composition order by checking whether the
  category string was in a hardcoded Python tuple:
      ('عام', 'اجتماعية', 'فن', 'متنوع', 'ثقافة', 'اقتصاد', 'تكنولوجيا')
  'سيارات' (Cars) was not in that list, so it fell into the ELSE branch
  which called:
      Image.alpha_composite(background, template)
  This re-composites the template ON TOP of the canvas after the news image
  had already been pasted. Because سيارات.png has a fully-opaque image-box
  area, it permanently covered the news image with white.

ROOT CAUSE B — 'ال'-prefix mismatch in category strings:
  settings.py uses source labels WITH the definite article prefix:
      "الاقتصاد" (economy), "الإمارات" (UAE), etc.
  But TEMPLATE_CONFIG keys in app3.py are WITHOUT it:
      "اقتصاد", "سياسة", etc.
  When the raw source_label flowed into generate_post_image as the
  'category' argument, template_config.get("الاقتصاد") returned None,
  silently falling back to the 'عام' config (wrong image_box coordinates).
  The 'عام' box (46,188,1040,744) is opaque → same cover-up problem.

ROOT CAUSE C — Fragile name-based branching:
  Any future category addition or rename would silently break composition
  order. The list had to be kept in perfect sync with TEMPLATE_CONFIG,
  with no code-level enforcement of that invariant.

FIX — Automatic transparency detection:
  Replaced the fragile name list with _template_has_transparent_image_box().
  This function samples the actual alpha channel of the loaded template
  image inside the image_box region and determines the correct composition
  order at runtime:

  • Transparent image-box (>50% transparent pixels) → HOLE style:
    Sports (رياضة), Politics (سياسة).
    Order: paste news image first → paste template on top.
    The transparent hole in the template reveals the news image below.

  • Opaque image-box (<50% transparent pixels) → FRAME style:
    Economy, Cars, Tech, Culture, Arts, General (عام).
    Order: paste template first → paste news image on top.
    The news image directly covers the opaque placeholder area.

  This detection is automatic, requires no list maintenance, and is
  correct for every template regardless of category name.
"""
from __future__ import annotations

import io
import os
import time
import traceback
from typing import Callable, Optional

import numpy as np
from PIL import Image, ImageDraw, ImageFilter, UnidentifiedImageError
from io import BytesIO

from utils.text_filter import sanitize_text
from image.text_formatter import prepare_ar_text, fit_text
from utils.logger import logger


# ─────────────────────────────────────────────────────────────────────────────
# Font resolution
# ─────────────────────────────────────────────────────────────────────────────

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_FONT_CANDIDATES = [
    os.path.join(_BASE_DIR, "Cairo-ExtraBold.ttf"),
    os.path.join(_BASE_DIR, "Cairo-Bold.ttf"),
    os.path.join(_BASE_DIR, "Cairo-Black.ttf"),
]

FONT_PATH: str = next((p for p in _FONT_CANDIDATES if os.path.exists(p)), "")
if not FONT_PATH:
    raise FileNotFoundError(
        "No Cairo font found. Expected one of: " + ", ".join(_FONT_CANDIDATES)
    )

logger.info(f"🔤 Using font: {os.path.basename(FONT_PATH)}")

MAX_FONT_SIZE = 60
MIN_FONT_SIZE = 26
TEXT_COLOR    = (255, 255, 255)

# ─────────────────────────────────────────────────────────────────────────────
# Download constants
# ─────────────────────────────────────────────────────────────────────────────

_MAX_RETRIES      = 5     # total download attempts
_RETRY_BASE_DELAY = 1.5   # exponential: 1.5 → 3 → 6 → 12 → 24 s
_DOWNLOAD_TIMEOUT = 20    # seconds per attempt

# Shown whenever article has no image or every download attempt fails.
# Dark charcoal — visually clean on all templates, never leaves a white gap.
_FALLBACK_BOX_COLOR = (30, 30, 30, 255)

# Fraction threshold for "mostly transparent" detection.
# If > this proportion of image_box pixels have alpha < 10, the template
# is treated as HOLE style (transparent cutout for image to show through).
_TRANSPARENT_THRESHOLD = 0.50


# ─────────────────────────────────────────────────────────────────────────────
# ISSUE #1 FIX — Template transparency detection
# ─────────────────────────────────────────────────────────────────────────────

def _template_has_transparent_image_box(
    template: Image.Image,
    image_box: tuple,
) -> bool:
    """
    Inspect the template's image_box region and return True if it is
    mostly transparent (hole style) or False if mostly opaque (frame style).

    This drives the composition order so the news image is always visible:

    True  → HOLE style  (Sports, Politics):
              paste news image first, then template on top.
    False → FRAME style (Economy, Cars, Tech, Culture, Arts, General, عام):
              paste template first (frame), then news image on top.

    The check is pixel-level and template-file-based, so it is correct
    regardless of category name strings and survives any future renames.
    """
    x1, y1, x2, y2 = image_box
    w, h = template.size
    x1, y1 = max(0, x1), max(0, y1)
    x2, y2 = min(w, x2), min(h, y2)

    if x2 <= x1 or y2 <= y1:
        return False   # degenerate box → treat as opaque / frame style

    arr = np.array(template.convert("RGBA"))
    alpha_region = arr[y1:y2, x1:x2, 3]  # alpha channel of image_box only
    transparent_fraction = (alpha_region < 10).mean()
    return bool(transparent_fraction > _TRANSPARENT_THRESHOLD)


# ─────────────────────────────────────────────────────────────────────────────
# Layer builders
# ─────────────────────────────────────────────────────────────────────────────

def _build_image_layer(
    canvas: Image.Image,
    news_img: Optional[Image.Image],
    image_box: tuple,
) -> Image.Image:
    """
    Render the article image (or dark-gray fallback) into image_box on canvas.

    Uses blurred-background + centered-foreground technique:
      • The news image is scaled and cropped to fill the entire box, then
        blurred to form a colour-matched background.
      • The sharp, aspect-ratio-preserved image is centered on top.

    When news_img is None:
      → Fill box with _FALLBACK_BOX_COLOR (dark charcoal).
      → Never leaves a white gap — template placeholder is always covered.
    """
    x1, y1, x2, y2 = image_box
    box_w = x2 - x1
    box_h = y2 - y1

    # ── Fallback: no image available ─────────────────────────────────────────
    if news_img is None:
        fallback = Image.new("RGBA", (box_w, box_h), _FALLBACK_BOX_COLOR)
        canvas.paste(fallback, (x1, y1), fallback)
        return canvas

    img_w, img_h = news_img.size
    img_ratio    = img_w / img_h
    box_ratio    = box_w / box_h

    # ── Blurred background fill ───────────────────────────────────────────────
    if img_ratio > box_ratio:
        bg_h, bg_w = box_h, int(box_h * img_ratio)
    else:
        bg_w, bg_h = box_w, int(box_w / img_ratio)

    bg  = news_img.resize((bg_w, bg_h), Image.Resampling.LANCZOS).convert("RGBA")
    bx  = (bg_w - box_w) // 2
    by  = (bg_h - box_h) // 2
    bg  = bg.crop((bx, by, bx + box_w, by + box_h))
    bg  = bg.filter(ImageFilter.GaussianBlur(radius=20))
    # Slight dark overlay to ensure text contrast
    overlay = Image.new("RGBA", (box_w, box_h), (0, 0, 0, 80))
    bg  = Image.alpha_composite(bg, overlay)

    # ── Sharp foreground centered ─────────────────────────────────────────────
    if img_ratio > box_ratio:
        fg_w, fg_h = box_w, int(box_w / img_ratio)
    else:
        fg_h, fg_w = box_h, int(box_h * img_ratio)

    fg   = news_img.resize((fg_w, fg_h), Image.Resampling.LANCZOS).convert("RGBA")
    fg_x = (box_w - fg_w) // 2
    fg_y = (box_h - fg_h) // 2

    # Composite blurred bg + sharp fg into a single layer
    layer = Image.new("RGBA", (box_w, box_h), (0, 0, 0, 0))
    layer.paste(bg, (0, 0))
    layer.paste(fg, (fg_x, fg_y), fg)

    # Paste the finished layer onto the canvas using its own alpha as mask
    canvas.paste(layer, (x1, y1), layer)
    return canvas


def _draw_title(
    final_img: Image.Image,
    title: str,
    text_box: tuple,
) -> Image.Image:
    """Draw the Arabic title text into text_box, centered, with shadow."""
    x1, y1, x2, y2 = text_box
    box_w = x2 - x1
    box_h = y2 - y1

    draw     = ImageDraw.Draw(final_img)
    title    = sanitize_text(title)
    ar_title = prepare_ar_text(title)

    font, lines, line_height = fit_text(
        draw, ar_title, FONT_PATH,
        box_w, box_h,
        MAX_FONT_SIZE, MIN_FONT_SIZE,
    )

    if not font:
        logger.error("❌ FONT FIT ERROR — title too long for any supported size")
        return final_img

    assert lines is not None and line_height is not None

    lines.reverse()
    total_h = len(lines) * line_height
    y       = y1 + ((box_h - total_h) // 2)

    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        w    = bbox[2] - bbox[0]
        x    = x1 + ((box_w - w) // 2)

        # Drop shadow
        draw.text((x + 2, y + 2), line, font=font, fill=(0, 0, 0))
        # Main text (with thin stroke for crispness)
        draw.text(
            (x, y), line, font=font,
            fill=TEXT_COLOR,stroke_width=1, stroke_fill=TEXT_COLOR
        )
        y += line_height

    return final_img


# ─────────────────────────────────────────────────────────────────────────────
# Image download with exponential retry
# ─────────────────────────────────────────────────────────────────────────────

def _download_news_image(
    session,
    image_url: str,
    news_id: int,
) -> Optional[Image.Image]:
    """
    Download the article's image with exponential back-off retry.

    Returns a PIL Image on success.
    Returns None after all retries fail — caller shows dark-gray fallback.
    """
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = session.get(image_url, timeout=_DOWNLOAD_TIMEOUT)
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            if "image" not in content_type and len(resp.content) < 1000:
                raise ValueError(
                    f"Response doesn't look like an image "
                    f"(Content-Type={content_type!r}, {len(resp.content)} bytes)"
                )

            img = Image.open(BytesIO(resp.content)).convert("RGBA")
            logger.info(
                f"✅ Image downloaded | id={news_id} | "
                f"attempt={attempt}/{_MAX_RETRIES} | size={img.size}"
            )
            return img

        except UnidentifiedImageError as exc:
            # Corrupt bytes — retrying won't help
            logger.warning(
                f"⚠️ Corrupt image data | id={news_id} "
                f"attempt={attempt}/{_MAX_RETRIES}: {exc}"
            )
            break

        except Exception as exc:
            delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
            logger.warning(
                f"⚠️ Image download failed | id={news_id} "
                f"attempt={attempt}/{_MAX_RETRIES}: {type(exc).__name__}: {exc}"
            )
            if attempt < _MAX_RETRIES:
                logger.info(f"   ↳ Retrying in {delay:.1f}s …")
                time.sleep(delay)

    logger.error(
        f"❌ All {_MAX_RETRIES} download attempts failed for id={news_id} "
        f"url={image_url!r} — using fallback background"
    )
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def generate_post_image(
    title: str,
    image_url: Optional[str],
    news_id: int,
    url: str,
    category: str,
    confidence: float,
    content: Optional[str],
    template_config: dict,
    session,
    upload_fn: Callable,
    supabase_storage,
    send_telegram_fn: Optional[Callable] = None,
    send_to_telegram: bool = True,
) -> Optional[str]:
    """
    Generate a branded post image and upload it to Supabase storage.

    Returns the public Supabase URL on success, or None on hard failure
    (missing template file, upload error — these prevent any usable output).

    Image download failure is NOT a hard failure: the dark fallback is used
    and the post still goes out with a clean branded image.

    Composition order (ISSUE #1 FIX):
    ───────────────────────────────────
    Determined automatically by inspecting the template's alpha channel:

    HOLE style (transparent image_box — Sports, Politics):
      1. Black base canvas
      2. Paste news image at image_box position
      3. Alpha-composite template on top
         → Template's transparent hole reveals the news image underneath

    FRAME style (opaque image_box — Economy, Cars, Tech, Culture, Arts, عام):
      1. Blank RGBA canvas
      2. Paste template as background/frame
      3. Paste news image directly on top at image_box position
         → News image overwrites the template's solid placeholder area

    This requires no category-name lists and is correct for every template.
    """
    logger.info(
        f"🎨 Generating image | id={news_id} | category={category!r} | "
        f"image_url={'set' if image_url else 'NONE'}"
    )

    try:
        # ── Template config lookup ────────────────────────────────────────────
        # Try exact category match first, then strip any leading 'ال' prefix,
        # then fall back to 'عام'.  This handles the source_label mismatch
        # (e.g. 'الاقتصاد' vs 'اقتصاد') without breaking anything.
        config = (
            template_config.get(category)
            or template_config.get(category.lstrip("ال"))
            or template_config.get("عام")
        )
        if not config:
            logger.error(
                f"❌ No template config for category={category!r} "
                f"and no 'عام' fallback — aborting"
            )
            return None

        image_box     = config["image_box"]
        text_box      = config["text_box"]
        template_path = config.get("template", "")

        if not template_path or not os.path.exists(template_path):
            logger.error(f"❌ Template file not found: {template_path!r}")
            return None

        template = Image.open(template_path).convert("RGBA")

        # ── Article image download ────────────────────────────────────────────
        news_img: Optional[Image.Image] = None

        if image_url:
            news_img = _download_news_image(session, image_url, news_id)
        else:
            logger.info(
                f"ℹ️  No image_url for id={news_id} — using fallback background"
            )

        # ── Detect template composition style (ISSUE #1 FIX) ─────────────────
        is_hole_style = _template_has_transparent_image_box(template, image_box)

        logger.debug(
            f"🖼  Template style for id={news_id} | category={category!r} | "
            f"{'HOLE (transparent → image below template)' if is_hole_style else 'FRAME (opaque → image above template)'}"
        )

        # ── Compose layers ────────────────────────────────────────────────────
        if is_hole_style:
            # HOLE style: news image first, template on top.
            # Template's transparent cutout reveals the news image below it.
            base = Image.new("RGBA", template.size, (0, 0, 0, 255))
            base = _build_image_layer(base, news_img, image_box)
            final_img = Image.alpha_composite(base, template)

        else:
            # FRAME style: template first as background/border frame,
            # news image pasted on top — directly overwrites the opaque box.
            base = Image.new("RGBA", template.size, (0, 0, 0, 0))
            base.paste(template, (0, 0))
            base = _build_image_layer(base, news_img, image_box)
            final_img = base

        # ── Draw Arabic title text ────────────────────────────────────────────
        final_img = _draw_title(final_img, title, text_box)

        # ── Upload to Supabase ────────────────────────────────────────────────
        filename   = f"news_{news_id}.jpg"
        upload_fn(final_img, filename)
        public_url = supabase_storage.from_("generated").get_public_url(filename)

        used_source = "article image" if news_img else "fallback background"
        logger.info(
            f"✅ Image ready | id={news_id} | "
            f"style={'hole' if is_hole_style else 'frame'} | "
            f"source={used_source} | url={public_url}"
        )

        # ── Optional Telegram raw image send ─────────────────────────────────
        if send_to_telegram and send_telegram_fn:
            buf = io.BytesIO()
            final_img.convert("RGB").save(buf, format="JPEG", quality=85)
            buf.seek(0)
            send_telegram_fn(buf, title, url, category, confidence, content or "")
            logger.info(f"🖼️  Telegram image sent | id={news_id}")

        return public_url

    except Exception as exc:
        logger.error(f"❌ Image generation hard error | id={news_id}: {exc}")
        traceback.print_exc()
        return None
