import requests
from datetime import datetime
from database.db import get_telegram_channels, upsert_telegram_channel_post


def _to_datetime(ts: int):
    try:
        return datetime.fromtimestamp(int(ts))
    except Exception:
        return datetime.now()


def parse_telegram_updates(channel_ids: list[str] = None, limit: int = 300) -> int:
    """
    Подтягивает channel_post из Telegram Bot API getUpdates
    и сохраняет в telegram_posts.
    """
    channels = get_telegram_channels(active_only=True)
    if channel_ids:
        allowed = {str(c).strip() for c in channel_ids if str(c).strip()}
        channels = [c for c in channels if str(c.get("id")).strip() in allowed]
    if not channels:
        return 0

    saved = 0
    for ch in channels:
        token = (ch.get("bot_token") or "").strip()
        channel_id = str(ch.get("id") or "").strip()
        if not token or not channel_id:
            continue

        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{token}/getUpdates",
                params={"limit": int(limit), "allowed_updates": '["channel_post","edited_channel_post"]'},
                timeout=30,
            )
            if not resp.ok:
                continue
            payload = resp.json()
            if not payload.get("ok"):
                continue

            for upd in payload.get("result", []):
                post = upd.get("channel_post") or upd.get("edited_channel_post")
                if not post:
                    continue
                chat = post.get("chat", {})
                post_chat_id = str(chat.get("id", "")).strip()
                post_username = str(chat.get("username", "")).strip()
                if channel_id not in {post_chat_id, f"@{post_username}" if post_username else ""}:
                    continue
                text = post.get("text") or post.get("caption") or ""
                upsert_telegram_channel_post(
                    channel_id=channel_id,
                    message_id=int(post.get("message_id")),
                    text=text,
                    published_at=_to_datetime(post.get("date")),
                    views=int(post.get("views") or 0),
                )
                saved += 1
        except Exception:
            continue

    return saved
