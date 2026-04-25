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

def get_vk_session():
    # Для постинга нужен токен с правами 'wall'
    return vk_api.VkApi(token=VK_TOKEN).get_api()

def publish_next_post():
    """Находит самый приоритетный пост и публикует его."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # 1. Берем самый приоритетный и старый пост со статусом 'ready'
    cursor.execute("""
        SELECT id, suggested_text, priority, attachments 
        FROM post_queue 
        WHERE status = 'ready' 
        ORDER BY priority DESC, created_at ASC 
        LIMIT 1
    """)
    post = cursor.fetchone()

    if not post:
        print("[Publisher] Очередь пуста.")
        conn.close()
        return

    if not GROUP_ID:
        print("[Publisher] Ошибка: GROUP_ID не найден в .env")
        conn.close()
        return

    print(f"[Publisher] Публикуем пост ID {post['id']} (Приоритет: {post['priority']}) в группу {GROUP_ID}...")

    try:
        vk_session = vk_api.VkApi(token=VK_TOKEN)
        vk = vk_session.get_api()
        
        # owner_id для групп ВСЕГДА должен быть отрицательным
        target_id = -int(str(GROUP_ID).strip())
        
        # Подготовка вложений
        post_attachments = post.get('attachments')
        
        # Если вложение - локальный путь к файлу, загружаем его в VK перед постингом
        if post_attachments and os.path.exists(str(post_attachments)):
            try:
                from parser.vk_parser import upload_photo
                with open(post_attachments, 'rb') as f:
                    post_attachments = upload_photo(vk, f)
            except Exception as upload_err:
                print(f"[Publisher] Ошибка загрузки локального файла в VK: {upload_err}")
                # Если не удалось загрузить, пробуем постить без вложения или как есть
        
        vk.wall.post(
            owner_id=target_id,
            message=post['suggested_text'],
            attachments=post_attachments,
            from_group=1
        )
        
        # 3. Сохраняем пост в таблице со статусом 'posted' для отчетности
        cursor.execute("UPDATE post_queue SET status = 'posted' WHERE id = %s", (post['id'],))
        conn.commit()
        print(f"[Publisher] Пост ID {post['id']} опубликован и помечен как posted.")

    except Exception as e:
        print(f"[Publisher] Ошибка при публикации: {e}")
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # В реальной жизни тут будет цикл или запуск по расписанию
    publish_next_post()
