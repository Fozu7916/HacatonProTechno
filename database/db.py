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
            id            INT PRIMARY KEY,
            owner_id      BIGINT NOT NULL,
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
            id          INT PRIMARY KEY,
            post_id     INT NOT NULL,
            from_id     BIGINT NOT NULL,
            date        DATETIME NOT NULL,
            text        TEXT,
            likes       INT DEFAULT 0,
            FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
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
            `value` VARCHAR(255)
        )
    """)
    cursor.execute("INSERT IGNORE INTO settings (`key`, `value`) VALUES ('posts_per_day', '3')")

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

    # Мягкая миграция для старых БД
    for col_sql in [
        "ALTER TABLE post_queue ADD COLUMN title VARCHAR(255)",
        "ALTER TABLE post_queue ADD COLUMN author_code VARCHAR(20)",
        "ALTER TABLE post_queue ADD COLUMN approver_code VARCHAR(20)",
    ]:
        try:
            cursor.execute(col_sql)
        except Exception:
            pass

    conn.commit()
    cursor.close()
    conn.close()
    print("[DB] Все таблицы инициализированы.")

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
    cursor.execute("""
        INSERT INTO posts (id, owner_id, date, text, likes, reposts, views, comments, attachments)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            text = VALUES(text), likes = VALUES(likes), reposts = VALUES(reposts),
            views = VALUES(views), comments = VALUES(comments), attachments = VALUES(attachments)
    """, (
        post["id"], post["owner_id"], datetime.fromtimestamp(post["date"]),
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
    cursor.execute("""
        INSERT INTO post_comments (id, post_id, from_id, date, text, likes)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE text = VALUES(text), likes = VALUES(likes)
    """, (
        comment["id"], post_id, comment["from_id"],
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
):
    """Добавляет пост в очередь на публикацию."""
    title_val = title or ((text or "").splitlines()[0][:255] if text else "Без заголовка")
    conn = get_connection()
    cursor = conn.cursor()
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
        "Редактор": "R",
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
