import time
import os
import vk_api
from dotenv import load_dotenv
from database.db import get_connection

# Загружаем настройки
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Пробуем взять токен для постинга, если нет - берем общий
VK_TOKEN = os.getenv("VK_USER_TOKEN") or os.getenv("VK_TOKEN")
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

def publish_next_post(only_due: bool = False, target_groups: list[str] = None):
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
                SELECT id, suggested_text, priority, attachments, target_group_ids
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
                SELECT id, suggested_text, priority, attachments, target_group_ids
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
    raw_groups = (post.get("target_group_ids") or "").strip()
    if raw_groups:
        post_target_groups = [g.strip().replace("-", "") for g in raw_groups.split(",") if g.strip()]
    selected_groups = [str(g).strip().replace("-", "") for g in (target_groups or post_target_groups or get_target_group_ids()) if str(g).strip()]
    if not selected_groups:
        print("[Publisher] Ошибка: GROUP_ID/GROUP_IDS не найден в .env")
        cursor.close()
        conn.close()
        return False

    print(f"[Publisher] Публикуем пост ID {post['id']} (Приоритет: {post['priority']}) в группы: {', '.join(selected_groups)}...")

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
            token_for_group = group_tokens.get(str(group_id)) or VK_TOKEN
            vk = vk_api.VkApi(token=token_for_group).get_api()
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
