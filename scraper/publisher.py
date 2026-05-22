import json
import os

import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from utils.logger import logger

TOKEN   = os.getenv("TOKEN")
CHAT_ID = os.getenv("CHAT_ID")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
def send_photo(photo_file, title: str, url: str, category: str, confidence: float, content: str):
   
    api_url = f"https://api.telegram.org/bot{TOKEN}/sendPhoto"

    short_content = (content[:500] + "...") if len(content) > 500 else content

    caption = (
        f"📂 التصنيف: {category}\n"
        f"🎯 الثقة: {round(confidence * 100, 1)}%\n\n"
        f"📝 {short_content}\n\n"
        f"📌 اضغط على الزر لقراءة التفاصيل"
    )

    keyboard = {
        "inline_keyboard": [
            [{"text": "📖 Read More", "url": url}],
            [{"text": "🔗 Share", "url": f"https://t.me/share/url?url={url}&text={title}"}],
        ]
    }

    file_to_close = None
    try:
        if isinstance(photo_file, str):
            file_to_send = open(photo_file, "rb")
            file_to_close = file_to_send
        else:
            file_to_send = photo_file
            file_to_send.seek(0)

        resp = requests.post(
            api_url,
            data={
                "chat_id":      CHAT_ID,
                "caption":      caption,
                "reply_markup": json.dumps(keyboard),
                "parse_mode":   "HTML",
            },
            files={"photo": file_to_send},
            timeout=30,
        )
        resp.raise_for_status()
        logger.info("✅ Sent to Telegram")

    except Exception as exc:
        logger.error(f"❌ Telegram send error: {exc}")
        raise   

    finally:
        if file_to_close:
            file_to_close.close()
