import os
import pandas as pd
from database.db import get_connection, add_to_queue
from analytics.engine import calculate_priority

def process_incoming_post(text: str):
    """
    Принимает текст, анализирует его относительно истории и кладет в очередь.
    """
    print(f"[*] Анализ входящего поста: {text[:50]}...")
    
    # 1. Загружаем историю для сравнения
    conn = get_connection()
    query = "SELECT likes, reposts, views, comments FROM posts"
    try:
        history_df = pd.read_sql(query, conn)
        # Считаем ER для истории
        if not history_df.empty:
            total_interactions = history_df['likes'] + history_df['reposts'] + history_df['comments']
            history_df['er'] = (total_interactions / history_df['views'].replace(0, 1)) * 100
    except:
        history_df = pd.DataFrame()
    finally:
        conn.close()

    # 2. Оцениваем пост (имитируем структуру поста для движка)
    # Так как пост новый, views=0, но движок оценит его потенциал
    mock_post = {
        "text": text,
        "views": {"count": 0},
        "likes": {"count": 0},
        "reposts": {"count": 0},
        "comments": {"count": 0},
        "attachments": [] # Можно добавить логику поиска ссылок/картинок
    }
    
    priority, predicted_er = calculate_priority(mock_post, history_df)
    
    # 3. Сохраняем в очередь (вторая БД)
    add_to_queue(
        post_id=None, # Это новый пост, у него еще нет ID из VK
        text=text,
        priority=priority,
        er=predicted_er
    )
    
    print(f"[+] Пост добавлен в очередь. Приоритет: {priority}, Ожидаемый ER: {predicted_er}%")

if __name__ == "__main__":
    # Тестовый запуск
    test_text = "Это тестовый пост для волонтеров о новом мероприятии в это воскресенье!"
    process_incoming_post(test_text)
