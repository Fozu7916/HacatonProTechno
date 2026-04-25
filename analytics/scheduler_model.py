import pandas as pd
import os
from database.db import get_connection
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def get_best_slots():
    """Анализирует историю и возвращает лучшие часы для каждого дня недели."""
    conn = get_connection()
    query = "SELECT date, likes, reposts, views, comments FROM posts"
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty or len(df) < 10:
        # Дефолтное расписание, если данных мало
        return {i: [10, 14, 18] for i in range(7)}

    df['date'] = pd.to_datetime(df['date'])
    df['day_of_week'] = df['date'].dt.dayofweek
    df['hour'] = df['date'].dt.hour
    
    # Считаем ER
    df['er'] = (df['likes'] + df['reposts'] + df['comments']) / df['views'].replace(0, 1) * 100
    
    # Группируем по дню недели и часу, считаем средний ER
    stats = df.groupby(['day_of_week', 'hour'])['er'].mean().reset_index()
    
    from database.db import get_setting
    posts_per_day = int(get_setting('posts_per_day', 3))
    
    schedule = {}
    for day in range(7):
        day_stats = stats[stats['day_of_week'] == day].sort_values(by='er', ascending=False)
        best_hours = day_stats['hour'].head(posts_per_day).tolist()
        # Если данных по часам нет, добавляем стандартные
        if not best_hours:
            best_hours = [12, 18]
        schedule[day] = sorted(best_hours)
        
    return schedule

if __name__ == "__main__":
    print("[*] Анализ лучших временных слотов...")
    s = get_best_slots()
    days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    for day_idx, hours in s.items():
        print(f"{days[day_idx]}: {hours}")
