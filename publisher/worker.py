import time
from datetime import datetime
from publisher.scheduler import publish_next_post


def run_worker(check_interval_sec=30):
    """
    Фоновый процесс для автоматической публикации по scheduled_at.
    """
    print(f"[*] Воркер автопубликации запущен. Интервал проверки: {check_interval_sec} сек.")

    while True:
        try:
            now = datetime.now()
            published_any = False
            # Публикуем все просроченные/наступившие посты пачкой за один проход
            while publish_next_post(only_due=True):
                published_any = True
                time.sleep(0.5)
            if published_any:
                print(f"[{now.strftime('%H:%M:%S')}] Все доступные по времени посты опубликованы.")
                
        except Exception as e:
            print(f"[Worker Error] {e}")
        
        time.sleep(check_interval_sec)

if __name__ == "__main__":
    run_worker()
