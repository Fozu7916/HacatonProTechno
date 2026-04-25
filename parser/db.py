import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

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

    conn.commit()
    cursor.close()
    conn.close()
    print("[DB] Таблицы инициализированы.")


def upsert_post(post: dict):
    """Вставляет или обновляет пост."""
    import json
    from datetime import datetime

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO posts (id, owner_id, date, text, likes, reposts, views, comments, attachments)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            text        = VALUES(text),
            likes       = VALUES(likes),
            reposts     = VALUES(reposts),
            views       = VALUES(views),
            comments    = VALUES(comments),
            attachments = VALUES(attachments),
            fetched_at  = CURRENT_TIMESTAMP
    """, (
        post["id"],
        post["owner_id"],
        datetime.fromtimestamp(post["date"]),
        post.get("text", ""),
        post.get("likes", {}).get("count", 0),
        post.get("reposts", {}).get("count", 0),
        post.get("views", {}).get("count", 0),
        post.get("comments", {}).get("count", 0),
        json.dumps(post.get("attachments", []), ensure_ascii=False),
    ))

    conn.commit()
    cursor.close()
    conn.close()


def upsert_comment(comment: dict, post_id: int):
    """Вставляет или обновляет комментарий."""
    from datetime import datetime

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO post_comments (id, post_id, from_id, date, text, likes)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            text  = VALUES(text),
            likes = VALUES(likes)
    """, (
        comment["id"],
        post_id,
        comment["from_id"],
        datetime.fromtimestamp(comment["date"]),
        comment.get("text", ""),
        comment.get("likes", {}).get("count", 0),
    ))

    conn.commit()
    cursor.close()
    conn.close()
