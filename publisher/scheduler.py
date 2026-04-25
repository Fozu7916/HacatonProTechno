import time
import os
import vk_api
import requests
from dotenv import load_dotenv
from database.db import get_connection, add_telegram_post_log, get_setting

# Загружаем настройки
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# VK_USER_TOKEN читаем из settings (интерфейс), fallback на .env
VK_TOKEN = (os.getenv("VK_TOKEN") or "").strip()
GROUP_ID = os.getenv("GROUP_ID", "").strip().replace("-", "")

def get_target_group_ids() -> list[str]:
    """Возвращает группы для публикации: GROUP_IDS или GROUP_ID."""
    raw_ids = os.getenv("GROUP_IDS", "").strip()
    if raw_ids:
        return [gid.strip().replace("-", "") for gid in raw_ids.split(",") if gid.strip()]
    return [GROUP_ID] if GROUP_ID else []

def get_vk_session():
    # Для постинга нужен токен с правами 'wall'
    return vk_api.VkApi(token=VK_TOKEN).get_api()


def _tg_send_text(token: str, channel_id: str, text: str) -> int:
    api_url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(
        api_url,
        json={
            "chat_id": channel_id,
            "text": text or "",
            "disable_web_page_preview": False,
        },
        timeout=30,
    )
    if not resp.ok:
        raise ValueError(f"Ошибка TG ({channel_id}): {resp.status_code} {resp.text[:250]}")
    data = resp.json()
    if not data.get("ok"):
        raise ValueError(f"Ошибка TG ({channel_id}): {data}")
    return int(data["result"]["message_id"])


def _tg_send_file(token: str, channel_id: str, file_path: str, caption: str = "") -> int:
    lower = str(file_path).lower()
    method = "sendPhoto" if lower.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")) else "sendDocument"
    file_key = "photo" if method == "sendPhoto" else "document"
    api_url = f"https://api.telegram.org/bot{token}/{method}"
    with open(file_path, "rb") as fh:
        resp = requests.post(
            api_url,
            data={"chat_id": channel_id, "caption": caption[:1024] if caption else ""},
            files={file_key: fh},
            timeout=60,
        )
    if not resp.ok:
        raise ValueError(f"Ошибка TG файла ({channel_id}): {resp.status_code} {resp.text[:250]}")
    data = resp.json()
    if not data.get("ok"):
        raise ValueError(f"Ошибка TG файла ({channel_id}): {data}")
    return int(data["result"]["message_id"])

def publish_next_post(
    only_due: bool = False,
    target_groups: list[str] = None,
    target_telegram_channels: list[str] = None,
    allow_env_fallback: bool = True,
):
    """
    Находит и публикует следующий пост.
    only_due=True: публикует только посты, где scheduled_at <= NOW() (или scheduled_at IS NULL).
    Возвращает True, если публикация выполнена.
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # 1. Берем самый приоритетный и старый пост со статусом 'ready'
    if only_due:
        try:
            cursor.execute("""
                SELECT id, suggested_text, priority, attachments, target_group_ids, target_telegram_ids
                FROM post_queue
                WHERE status = 'ready'
                  AND (scheduled_at IS NULL OR scheduled_at <= NOW())
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            """)
        except Exception:
            cursor.execute("""
                SELECT id, suggested_text, priority, attachments
                FROM post_queue
                WHERE status = 'ready'
                  AND (scheduled_at IS NULL OR scheduled_at <= NOW())
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            """)
    else:
        try:
            cursor.execute("""
                SELECT id, suggested_text, priority, attachments, target_group_ids, target_telegram_ids
                FROM post_queue
                WHERE status = 'ready'
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            """)
        except Exception:
            cursor.execute("""
                SELECT id, suggested_text, priority, attachments
                FROM post_queue
                WHERE status = 'ready'
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            """)
    post = cursor.fetchone()

    if not post:
        print("[Publisher] Нет постов для публикации.")
        cursor.close()
        conn.close()
        return False

    # Атомарная защита от дублей:
    # помечаем пост как posted ТОЛЬКО если он все еще ready.
    # Если уже обработан другим кликом/процессом - выходим.
    lock_cur = conn.cursor()
    lock_cur.execute(
        "UPDATE post_queue SET status = 'posted' WHERE id = %s AND status = 'ready'",
        (post["id"],),
    )
    conn.commit()
    if lock_cur.rowcount == 0:
        lock_cur.close()
        print(f"[Publisher] Пост ID {post['id']} уже обработан другим процессом.")
        cursor.close()
        conn.close()
        return False
    lock_cur.close()

    post_target_groups = []
    post_target_telegram = []
    raw_groups = (post.get("target_group_ids") or "").strip()
    if raw_groups:
        post_target_groups = [g.strip().replace("-", "") for g in raw_groups.split(",") if g.strip()]
    raw_tg = (post.get("target_telegram_ids") or "").strip()
    if raw_tg:
        post_target_telegram = [c.strip() for c in raw_tg.split(",") if c and c.strip()]
    if target_groups is not None:
        groups_source = target_groups
    elif post_target_groups:
        groups_source = post_target_groups
    elif allow_env_fallback:
        groups_source = get_target_group_ids()
    else:
        groups_source = []
    selected_groups = [str(g).strip().replace("-", "") for g in groups_source if str(g).strip()]

    channels_source = target_telegram_channels if target_telegram_channels is not None else post_target_telegram
    selected_telegram_channels = [str(c).strip() for c in channels_source if str(c).strip()]
    if not selected_groups and not selected_telegram_channels:
        print("[Publisher] Ошибка: не выбраны каналы публикации (VK/TG)")
        cursor.close()
        conn.close()
        return False

    print(
        f"[Publisher] Публикуем пост ID {post['id']} (Приоритет: {post['priority']}) "
        f"в VK: {', '.join(selected_groups) if selected_groups else '-'}; "
        f"TG: {', '.join(selected_telegram_channels) if selected_telegram_channels else '-'}..."
    )

    try:
        group_tokens = {}
        if selected_groups:
            map_conn = get_connection()
            map_cur = map_conn.cursor(dictionary=True)
            placeholders = ", ".join(["%s"] * len(selected_groups))
            map_cur.execute(
                f"SELECT id, token FROM vk_groups WHERE id IN ({placeholders})",
                tuple(int(g) for g in selected_groups),
            )
            for row in map_cur.fetchall():
                group_tokens[str(row["id"])] = (row.get("token") or "").strip()
            map_cur.close()
            map_conn.close()
        
        # Подготовка вложений: может быть список через запятую
        attachments_raw = (post.get('attachments') or "").strip()
        attachment_items = [a.strip() for a in attachments_raw.split(",") if a.strip()]
        for group_id in selected_groups:
            vk_user_token = (get_setting("vk_user_token", "") or "").strip() or (os.getenv("VK_USER_TOKEN") or "").strip()
            if not vk_user_token:
                raise ValueError(
                    "Не задан VK_USER_TOKEN. Для публикации и загрузки вложений в VK нужен токен пользователя-админа."
                )
            vk = vk_api.VkApi(token=vk_user_token).get_api()
            target_id = -int(str(group_id).strip())
            resolved_attachments = []
            for item in attachment_items:
                if os.path.exists(item):
                    try:
                        from parser.vk_parser import upload_attachment
                        with open(item, 'rb') as f:
                            resolved_attachments.append(upload_attachment(vk, f, os.path.basename(item), group_id=group_id))
                    except Exception as upload_err:
                        print(f"[Publisher] Ошибка загрузки файла {item} в VK для группы {group_id}: {upload_err}")
                else:
                    resolved_attachments.append(item)
            post_attachments = ",".join(resolved_attachments) if resolved_attachments else None

            vk.wall.post(
                owner_id=target_id,
                message=post['suggested_text'],
                attachments=post_attachments,
                from_group=1
            )

        if selected_telegram_channels:
            tg_conn = get_connection()
            tg_cur = tg_conn.cursor(dictionary=True)
            placeholders = ", ".join(["%s"] * len(selected_telegram_channels))
            tg_cur.execute(
                f"SELECT id, bot_token FROM telegram_channels WHERE id IN ({placeholders})",
                tuple(selected_telegram_channels),
            )
            token_by_channel = {str(r["id"]): (r.get("bot_token") or "").strip() for r in tg_cur.fetchall()}
            tg_cur.close()
            tg_conn.close()

            for channel_id in selected_telegram_channels:
                token = token_by_channel.get(str(channel_id)) or os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
                if not token:
                    raise ValueError(f"Не найден bot token для TG канала {channel_id}")
                sent_any_file = False
                first_file_caption = post["suggested_text"] or ""
                for item in attachment_items:
                    if not os.path.exists(item):
                        continue
                    message_id = _tg_send_file(token, channel_id, item, caption=first_file_caption if not sent_any_file else "")
                    add_telegram_post_log(post["id"], channel_id, message_id=message_id, text=post["suggested_text"] or "", attachment=item)
                    sent_any_file = True
                if not sent_any_file:
                    message_id = _tg_send_text(token, channel_id, post["suggested_text"] or "")
                    add_telegram_post_log(post["id"], channel_id, message_id=message_id, text=post["suggested_text"] or "", attachment=None)
        
        print(f"[Publisher] Пост ID {post['id']} опубликован и помечен как posted.")
        return True

    except Exception as e:
        # Возвращаем в ready, если публикация в VK не удалась
        rollback_cur = conn.cursor()
        rollback_cur.execute(
            "UPDATE post_queue SET status = 'ready' WHERE id = %s AND status = 'posted'",
            (post["id"],),
        )
        conn.commit()
        rollback_cur.close()
        print(f"[Publisher] Ошибка при публикации: {e}")
        return False
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # В реальной жизни тут будет цикл или запуск по расписанию
    publish_next_post()
