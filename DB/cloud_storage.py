import io
import logging
import os

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

logger = logging.getLogger(__name__)

_SUPABASE_URL = os.getenv("SUPABASE_URL")
_SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not _SUPABASE_URL or not _SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in the environment")

_supabase = create_client(str(_SUPABASE_URL), str(_SUPABASE_KEY))


def upload_image(image, filename: str) -> bool:
    """Upload a PIL Image to the 'generated' Supabase bucket. Returns True on success."""
    image    = image.convert("RGB")
    filename = filename.replace(".png", ".jpg")

    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=85, optimize=True)
    buffer.seek(0)

    size_mb = len(buffer.getvalue()) / (1024 * 1024)
    logger.info(f"🖼️  Uploading {filename} ({size_mb:.2f} MB)")

    try:
        result = _supabase.storage.from_("generated").upload(
            path=filename,
            file=buffer.read(),
            file_options={"content-type": "image/jpeg"},
        )
        logger.debug(f"✅ Upload result: {result}")
        return True
    except Exception as exc:
        logger.error(f"❌ Upload failed for {filename}: {exc}")
        return False
