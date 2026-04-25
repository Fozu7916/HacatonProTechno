import mysql.connector
import os
import hashlib
from dotenv import load_dotenv

# Загружаем .env из корня (на два уровня выше)
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )

def init_db():
    conn = get_connection()
    cursor = conn.cursor()

    # Таблица постов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id            BIGINT PRIMARY KEY,
            owner_id      BIGINT NOT NULL,
            group_id      BIGINT,
            date          DATETIME NOT NULL,
            text          TEXT,
            likes         INT DEFAULT 0,
            reposts       INT DEFAULT 0,
            views         INT DEFAULT 0,
            comments      INT DEFAULT 0,
            attachments   JSON,
            fetched_at    DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Таблица комментариев
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS post_comments (
            id          BIGINT PRIMARY KEY,
            post_id     BIGINT NOT NULL,
            from_id     BIGINT NOT NULL,
            date        DATETIME NOT NULL,
            text        TEXT,
            likes       INT DEFAULT 0,
            FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
        )
    """)
    # Таблица групп VK
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vk_groups (
            id         INT PRIMARY KEY,
            name       VARCHAR(255) NOT NULL,
            token      VARCHAR(255) NOT NULL,
            tags       TEXT,
            is_active  TINYINT(1) DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Таблица Telegram-каналов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telegram_channels (
            id         VARCHAR(255) PRIMARY KEY,
            name       VARCHAR(255) NOT NULL,
            bot_token  VARCHAR(255) NOT NULL,
            tags       TEXT,
            is_active  TINYINT(1) DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Лог опубликованных сообщений Telegram
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS telegram_posts (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            queue_id      INT,
            channel_id    VARCHAR(255) NOT NULL,
            message_id    BIGINT,
            text          TEXT,
            attachment    VARCHAR(500),
            views         INT DEFAULT 0,
            published_at  DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Таблица очереди
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS post_queue (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            post_id       INT,
            title         VARCHAR(255),
            suggested_text TEXT,
            attachments   TEXT,
            target_group_ids TEXT,
            target_telegram_ids TEXT,
            priority      INT DEFAULT 1,
            author_role   ENUM('smm', 'volunteer') DEFAULT 'volunteer',
            author_code   VARCHAR(20),
            approver_code VARCHAR(20),
            scheduled_at  DATETIME DEFAULT NULL,
            predicted_er  FLOAT,
            status        ENUM('pending', 'editing', 'ready', 'posted') DEFAULT 'pending',
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Таблица шаблонов
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS templates (
            id      INT AUTO_INCREMENT PRIMARY KEY,
            name    VARCHAR(100),
            content TEXT
        )
    """)

    # Таблица настроек
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            `key`   VARCHAR(50) PRIMARY KEY,
            `value` TEXT
        )
    """)
    cursor.execute("INSERT IGNORE INTO settings (`key`, `value`) VALUES ('posts_per_day', '3')")
    cursor.execute("INSERT IGNORE INTO settings (`key`, `value`) VALUES ('vk_user_token', '')")

    # Таблица пользователей
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            code          VARCHAR(20) UNIQUE,
            full_name     VARCHAR(255) NOT NULL,
            email         VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            role          VARCHAR(50) NOT NULL,
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Таблица ожидающих регистраций
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pending_registrations (
            id            INT AUTO_INCREMENT PRIMARY KEY,
            full_name     VARCHAR(255) NOT NULL,
            email         VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            role          VARCHAR(50) NOT NULL,
            status        ENUM('pending', 'approved', 'rejected') DEFAULT 'pending',
            rejection_reason VARCHAR(500),
            created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
            reviewed_at   DATETIME,
            reviewed_by   VARCHAR(20)
        )
    """)
    # Мягкая миграция для старых БД
    for col_sql in [
        "ALTER TABLE posts ADD COLUMN group_id BIGINT",
        "ALTER TABLE vk_groups ADD COLUMN tags TEXT",
        "ALTER TABLE telegram_channels ADD COLUMN tags TEXT",
        "ALTER TABLE telegram_posts ADD COLUMN attachment VARCHAR(500)",
        "ALTER TABLE telegram_posts ADD COLUMN views INT DEFAULT 0",
        "ALTER TABLE settings MODIFY COLUMN `value` TEXT",
        "ALTER TABLE post_queue ADD COLUMN title VARCHAR(255)",
        "ALTER TABLE post_queue ADD COLUMN author_code VARCHAR(20)",
        "ALTER TABLE post_queue ADD COLUMN approver_code VARCHAR(20)",
        "ALTER TABLE post_queue ADD COLUMN target_group_ids TEXT",
        "ALTER TABLE post_queue ADD COLUMN target_telegram_ids TEXT",
    ]:
        try:
            cursor.execute(col_sql)
        except Exception:
            pass
    try:
        cursor.execute("ALTER TABLE telegram_posts ADD UNIQUE KEY uq_tg_channel_message (channel_id, message_id)")
    except Exception:
        pass

    conn.commit()
    cursor.close()
    conn.close()
    
    # Создаем автоадминистратора
    create_auto_admin()
    
    # Автоматически одобряем все ожидающие заявки
    auto_approve_pending_requests()
    
    print("[DB] Все таблицы инициализированы.")


def get_vk_groups(active_only: bool = True):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    if active_only:
        cursor.execute("SELECT id, name, token, tags, is_active FROM vk_groups WHERE is_active = 1 ORDER BY id")
    else:
        cursor.execute("SELECT id, name, token, tags, is_active FROM vk_groups ORDER BY id")
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res


def upsert_vk_group(group_id: int, name: str, token: str, tags: str = "", is_active: bool = True):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO vk_groups (id, name, token, tags, is_active)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            token = VALUES(token),
            tags = VALUES(tags),
            is_active = VALUES(is_active)
        """,
        (group_id, name, token, (tags or "").strip(), 1 if is_active else 0),
    )
    conn.commit()
    cursor.close()
    conn.close()


def get_telegram_channels(active_only: bool = True):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        if active_only:
            cursor.execute("SELECT id, name, bot_token, tags, is_active FROM telegram_channels WHERE is_active = 1 ORDER BY name")
        else:
            cursor.execute("SELECT id, name, bot_token, tags, is_active FROM telegram_channels ORDER BY name")
        res = cursor.fetchall()
        cursor.close()
        conn.close()
        return res
    except Exception:
        cursor.close()
        conn.close()
        return []


def upsert_telegram_channel(channel_id: str, name: str, bot_token: str, tags: str = "", is_active: bool = True):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO telegram_channels (id, name, bot_token, tags, is_active)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            bot_token = VALUES(bot_token),
            tags = VALUES(tags),
            is_active = VALUES(is_active)
        """,
        ((channel_id or "").strip(), (name or "").strip(), (bot_token or "").strip(), (tags or "").strip(), 1 if is_active else 0),
    )
    conn.commit()
    cursor.close()
    conn.close()


def add_telegram_post_log(queue_id: int, channel_id: str, message_id: int = None, text: str = None, attachment: str = None):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO telegram_posts (queue_id, channel_id, message_id, text, attachment)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (queue_id, str(channel_id), message_id, text, attachment),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def upsert_telegram_channel_post(channel_id: str, message_id: int, text: str = None, published_at=None, views: int = 0):
    """Сохраняет пост, полученный из Telegram-канала (через getUpdates)."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO telegram_posts (channel_id, message_id, text, published_at, views)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                text = VALUES(text),
                published_at = VALUES(published_at),
                views = VALUES(views)
            """,
            (str(channel_id), int(message_id), text, published_at, int(views or 0)),
        )
        conn.commit()
    finally:
        cursor.close()
        conn.close()


def get_telegram_stats(days: int = 30):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_posts,
                COUNT(DISTINCT channel_id) AS channels_count
            FROM telegram_posts
            WHERE published_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
            """,
            (int(days),),
        )
        row = cursor.fetchone() or {}
        cursor.execute(
            """
            SELECT channel_id, COUNT(*) AS cnt
            FROM telegram_posts
            WHERE published_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
            GROUP BY channel_id
            ORDER BY cnt DESC
            LIMIT 1
            """,
            (int(days),),
        )
        top = cursor.fetchone()
        return {
            "total_posts": int(row.get("total_posts") or 0),
            "channels_count": int(row.get("channels_count") or 0),
            "top_channel": (top or {}).get("channel_id"),
            "top_channel_count": int((top or {}).get("cnt") or 0),
        }
    finally:
        cursor.close()
        conn.close()


def get_setting(key, default=None):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT `value` FROM settings WHERE `key` = %s", (key,))
    res = cursor.fetchone()
    cursor.close()
    conn.close()
    return res[0] if res else default

def set_setting(key, value):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("REPLACE INTO settings (`key`, `value`) VALUES (%s, %s)", (key, str(value)))
    conn.commit()
    cursor.close()
    conn.close()

def get_templates():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM templates")
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res

def add_template(name, content):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO templates (name, content) VALUES (%s, %s)", (name, content))
    conn.commit()
    cursor.close()
    conn.close()

def upsert_post(post: dict):
    import json
    from datetime import datetime
    conn = get_connection()
    cursor = conn.cursor()
    # Составляем полный ID поста: owner_id_post_id
    full_post_id = int(str(post["owner_id"]).replace("-", "") + str(post["id"]).zfill(10))
    group_id = abs(int(post["owner_id"]))
    # Для БД со связью posts.group_id -> vk_groups.id:
    # гарантируем, что запись о группе существует.
    cursor.execute("""
        INSERT INTO vk_groups (id, name, token)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE id = id
    """, (group_id, f"Группа {group_id}", ""))
    cursor.execute("""
        INSERT INTO posts (id, owner_id, group_id, date, text, likes, reposts, views, comments, attachments)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            owner_id = VALUES(owner_id), group_id = VALUES(group_id), text = VALUES(text), likes = VALUES(likes), reposts = VALUES(reposts),
            views = VALUES(views), comments = VALUES(comments), attachments = VALUES(attachments)
    """, (
        full_post_id, post["owner_id"], group_id, datetime.fromtimestamp(post["date"]),
        post.get("text", ""), post.get("likes", {}).get("count", 0),
        post.get("reposts", {}).get("count", 0), post.get("views", {}).get("count", 0),
        post.get("comments", {}).get("count", 0),
        json.dumps(post.get("attachments", []), ensure_ascii=False),
    ))
    conn.commit()
    cursor.close()
    conn.close()
def upsert_comment(comment: dict, post_id: int):
    from datetime import datetime
    conn = get_connection()
    cursor = conn.cursor()
    # Составляем полный ID комментария: post_id_comment_id
    full_comment_id = int(str(post_id) + str(comment["id"]).zfill(10))
    cursor.execute("""
        INSERT INTO post_comments (id, post_id, from_id, date, text, likes)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE text = VALUES(text), likes = VALUES(likes)
    """, (
        full_comment_id, post_id, comment["from_id"],
        datetime.fromtimestamp(comment["date"]), comment.get("text", ""),
        comment.get("likes", {}).get("count", 0),
    ))
    conn.commit()
    cursor.close()
    conn.close()
def add_to_queue(
    post_id: int,
    text: str,
    priority: int,
    er: float,
    scheduled_at: datetime = None,
    attachments: str = None,
    title: str = None,
    author_code: str = None,
    approver_code: str = None,
    target_group_ids: str = None,
    target_telegram_ids: str = None,
):
    """Добавляет пост в очередь на публикацию."""
    title_val = title or ((text or "").splitlines()[0][:255] if text else "Без заголовка")
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO post_queue (post_id, title, suggested_text, priority, predicted_er, scheduled_at, attachments, author_code, approver_code, target_group_ids, target_telegram_ids)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (post_id, title_val, text, priority, er, scheduled_at, attachments, author_code, approver_code, target_group_ids, target_telegram_ids))
    except Exception:
        try:
            cursor.execute("""
                INSERT INTO post_queue (post_id, title, suggested_text, priority, predicted_er, scheduled_at, attachments, author_code, approver_code, target_group_ids)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (post_id, title_val, text, priority, er, scheduled_at, attachments, author_code, approver_code, target_group_ids))
        except Exception:
            cursor.execute("""
                INSERT INTO post_queue (post_id, title, suggested_text, priority, predicted_er, scheduled_at, attachments, author_code, approver_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (post_id, title_val, text, priority, er, scheduled_at, attachments, author_code, approver_code))
    conn.commit()
    cursor.close()
    conn.close()


def _role_prefix(role: str) -> str:
    mapping = {
        "Руководитель": "R",
        "Администратор": "A",
        "Редактор": "E",
        "Волонтер": "B",
        "СММ-специалист": "C",
        "СММ": "C",
        "Наблюдатель": "N",
    }
    return mapping.get(role, "U")


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def create_user(full_name: str, email: str, password: str, role: str):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    prefix = _role_prefix(role)
    cursor.execute("SELECT COUNT(*) as c FROM users WHERE role = %s", (role,))
    count = int(cursor.fetchone()["c"]) + 1
    code = f"{prefix}{count}"
    try:
        cursor.execute(
            "INSERT INTO users (code, full_name, email, password_hash, role) VALUES (%s, %s, %s, %s, %s)",
            (code, full_name, email, _hash_password(password), role),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()
    return code


def authenticate_user(email: str, password: str):
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT id, code, full_name, email, role, password_hash FROM users WHERE email = %s",
        (email,),
    )
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    if not user:
        return None
    if user["password_hash"] != _hash_password(password):
        return None
    user.pop("password_hash", None)
    return user


def create_auto_admin():
    """Создает автоадминистратора при инициализации БД."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Проверяем, существует ли уже автоадминистратор
        cursor.execute("SELECT id FROM users WHERE email = %s", ("root",))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return "Автоадминистратор уже существует"
        
        # Создаем автоадминистратора
        code = "AUTO_ADMIN_1"
        cursor.execute(
            "INSERT INTO users (code, full_name, email, password_hash, role) VALUES (%s, %s, %s, %s, %s)",
            (code, "Автоадминистратор", "root", _hash_password("root"), "Администратор"),
        )
        conn.commit()
        cursor.close()
        conn.close()
        return f"Автоадминистратор создан: {code}"
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        raise e

def auto_approve_pending_requests():
    """Автоматически одобряет все ожидающие заявки."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Получаем ID автоадминистратора
        cursor.execute("SELECT code FROM users WHERE email = %s", ("auto_admin@system.local",))
        admin_result = cursor.fetchone()
        if not admin_result:
            cursor.close()
            conn.close()
            return 0
        
        admin_code = admin_result[0]
        
        # Обновляем все pending заявки на ready и устанавливаем approver_code
        cursor.execute(
            "UPDATE post_queue SET status = %s, approver_code = %s WHERE status = %s",
            ("ready", admin_code, "pending"),
        )
        conn.commit()
        affected_rows = cursor.rowcount
        cursor.close()
        conn.close()
        return affected_rows
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        raise e


def update_user_credentials(email: str, new_password: str = None, new_email: str = None):
    """Обновляет учетные данные пользователя."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        if new_email and new_password:
            cursor.execute(
                "UPDATE users SET email = %s, password_hash = %s WHERE email = %s",
                (new_email, _hash_password(new_password), email),
            )
        elif new_password:
            cursor.execute(
                "UPDATE users SET password_hash = %s WHERE email = %s",
                (_hash_password(new_password), email),
            )
        elif new_email:
            cursor.execute(
                "UPDATE users SET email = %s WHERE email = %s",
                (new_email, email),
            )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        raise e


def create_pending_registration(full_name: str, email: str, password: str, role: str):
    """Создает заявку на регистрацию, требующую одобрения администратора."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO pending_registrations (full_name, email, password_hash, role) VALUES (%s, %s, %s, %s)",
            (full_name, email, _hash_password(password), role),
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        raise e


def get_pending_registrations():
    """Получает все ожидающие регистрации."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM pending_registrations WHERE status = 'pending' ORDER BY created_at DESC")
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    return res


def approve_registration(registration_id: int, admin_code: str):
    """Одобряет регистрацию и создает пользователя."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Получаем данные заявки
        cursor.execute("SELECT * FROM pending_registrations WHERE id = %s", (registration_id,))
        reg = cursor.fetchone()
        if not reg:
            raise ValueError("Заявка не найдена")
        
        # Создаем пользователя
        prefix = _role_prefix(reg["role"])
        cursor.execute("SELECT COUNT(*) as c FROM users WHERE role = %s", (reg["role"],))
        count = int(cursor.fetchone()["c"]) + 1
        code = f"{prefix}{count}"
        
        cursor.execute(
            "INSERT INTO users (code, full_name, email, password_hash, role) VALUES (%s, %s, %s, %s, %s)",
            (code, reg["full_name"], reg["email"], reg["password_hash"], reg["role"]),
        )
        
        # Обновляем статус заявки
        cursor.execute(
            "UPDATE pending_registrations SET status = %s, reviewed_at = NOW(), reviewed_by = %s WHERE id = %s",
            ("approved", admin_code, registration_id),
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        return code
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        raise e


def reject_registration(registration_id: int, admin_code: str, reason: str = ""):
    """Отклоняет регистрацию."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "UPDATE pending_registrations SET status = %s, rejection_reason = %s, reviewed_at = NOW(), reviewed_by = %s WHERE id = %s",
            ("rejected", reason, admin_code, registration_id),
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        raise e
