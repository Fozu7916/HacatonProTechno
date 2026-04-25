import time
import os
from datetime import datetime
from publisher.scheduler import publish_next_post
from analytics.scheduler_model import get_best_slots

def run_worker(check_interval_sec=60):
    """
    Фоновый процесс для автоматической публикации в 'золотые часы'.
    """
    print(f"[*] Умный воркер запущен.")
    last_posted_hour = -1

    while True:
        try:
            now = datetime.now()
            current_day = now.weekday()
            current_hour = now.hour

            # Проверяем расписание только если минута сменилась (или чаще)
            now = datetime.now()
            current_day = now.weekday()
            current_hour = now.hour

            # 1. Сначала ищем посты с жестко заданным временем, которое уже наступило
            conn = get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT id FROM post_queue 
                WHERE scheduled_at <= %s AND status = 'pending' 
                ORDER BY scheduled_at ASC LIMIT 1
            """, (now,))
            manual_post = cursor.fetchone()
            cursor.close()
            conn.close()

            if manual_post:
                print(f"[{now.strftime('%H:%M')}] Публикация запланированного поста (по времени)...")
                publish_next_post()
                last_posted_hour = current_hour
            
            # 2. Если ручных на текущий момент нет, проверяем "золотые часы" для волонтеров
            elif current_hour != last_posted_hour:
                schedule = get_best_slots()
                best_hours = schedule.get(current_day, [12, 18])

                if current_hour in best_hours:
                    print(f"[{now.strftime('%H:%M')}] Наступило удачное время для поста ({current_hour}:00)!")
                    publish_next_post()
                    last_posted_hour = current_hour
                
        except Exception as e:
            print(f"[Worker Error] {e}")
        
        time.sleep(check_interval_sec)

if __name__ == "__main__":
    run_worker()
