import pandas as pd
import numpy as np

def calculate_priority(post: dict, history_df: pd.DataFrame = None) -> tuple[int, float]:
    """
    Оценивает пост и возвращает (приоритет 1-5, предсказанный ER).
    Логика:
    - Если ER выше среднего по истории -> приоритет выше.
    - Если есть вложения (фото/видео) -> приоритет выше.
    - Длинные тексты в этой тематике могут иметь другой приоритет.
    """
    # Базовый ER на основе текущих показателей (если пост уже имеет охват)
    views = post.get("views", {}).get("count", 1)
    interactions = (post.get("likes", {}).get("count", 0) + 
                    post.get("reposts", {}).get("count", 0) + 
                    post.get("comments", {}).get("count", 0))
    
    current_er = (interactions / (views if views > 0 else 1)) * 100
    
    # Базовый приоритет
    priority = 1
    
    # 1. Проверка на вложения
    attachments = post.get("attachments", [])
    if len(attachments) > 0:
        priority += 1
        
    # 2. Сравнение с историей (если есть)
    if history_df is not None and not history_df.empty:
        avg_er = history_df['er'].mean()
        if current_er > avg_er * 1.5:
            priority += 2
        elif current_er > avg_er:
            priority += 1
            
    # Ограничиваем приоритет максимумом 5
    priority = min(priority, 5)
    
    return int(priority), round(float(current_er), 2)
