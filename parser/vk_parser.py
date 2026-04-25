import time
import os
import vk_api
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

VK_TOKEN = os.getenv("VK_SERVICE_TOKEN") or os.getenv("VK_TOKEN")
GROUP_ID = os.getenv("GROUP_ID")  # числовой ID группы (без минуса)


def get_vk_session() -> vk_api.VkApi:
    session = vk_api.VkApi(token=VK_TOKEN)
    return session


def fetch_posts(vk, count: int = 100, offset: int = 0) -> list[dict]:
    """
    Получает посты из сообщества.
    count  — сколько постов за один запрос (макс. 100)
    offset — смещение (для пагинации)
    """
    response = vk.wall.get(
        owner_id=f"-{GROUP_ID}",
        count=count,
        offset=offset,
        extended=0,
    )
    return response.get("items", [])


def fetch_comments(vk, post_id: int, count: int = 100) -> list[dict]:
    """Получает комментарии к посту."""
    all_comments = []
    offset = 0

    while True:
        response = vk.wall.getComments(
            owner_id=f"-{GROUP_ID}",
            post_id=post_id,
            count=count,
            offset=offset,
            extended=0,
            thread_items_count=0,
        )
        items = response.get("items", [])
        all_comments.extend(items)

        if len(items) < count:
            break

        offset += count
        time.sleep(0.34)  # соблюдаем лимит VK API (3 запроса/сек)

    return all_comments


def get_photo_url(vk, photo_id):
    """Получает прямую ссылку на изображение по его VK ID."""
    try:
        # photo_id имеет формат photo123_456
        pid = photo_id.replace("photo", "")
        # Явно указываем версию API и параметры
        res = vk.photos.getById(photos=pid, extended=0)
        if res and len(res) > 0:
            # Ищем самый большой размер (обычно тип 'w' или 'z')
            sizes = res[0].get('sizes', [])
            if sizes:
                # Сортируем по ширине и берем максимум
                best_size = sorted(sizes, key=lambda x: x.get('width', 0))[-1]
                return best_size.get('url')
        return None
    except Exception as e:
        print(f"[VK Photo URL Error] {e}")
        return None

def upload_photo(vk, photo_file):
    """Загружает фото на сервера VK и возвращает строку вложения."""
    import requests
    gid = str(os.getenv("GROUP_ID", "")).strip().replace("-", "")
    
    try:
        # Получаем сервер для загрузки именно на стену ГРУППЫ
        upload_server = vk.photos.getWallUploadServer(group_id=gid)
        upload_url = upload_server['upload_url']
        
        # Отправляем файл
        files = {'photo': photo_file}
        response = requests.post(upload_url, files=files).json()
        
        # Сохраняем фото в альбом группы
        save_res = vk.photos.saveWallPhoto(
            group_id=gid,
            photo=response['photo'],
            server=response['server'],
            hash=response['hash']
        )[0]
        
        # Возвращаем ID, который будет доступен всем админам группы
        return f"photo{save_res['owner_id']}_{save_res['id']}"
    except Exception as e:
        print(f"[VK Upload Error] {e}")
        raise e


def parse_all_posts(n: int = None) -> list[dict]:
    """
    Парсит все (или последние n) постов сообщества вместе с комментариями.
    Возвращает список словарей: {"post": ..., "comments": [...]}
    """
    session = get_vk_session()
    vk = session.get_api()

    result = []
    offset = 0
    batch = 100
    fetched = 0

    while True:
        to_fetch = batch if n is None else min(batch, n - fetched)
        posts = fetch_posts(vk, count=to_fetch, offset=offset)

        if not posts:
            break

        for post in posts:
            comments = []
            if post.get("comments", {}).get("count", 0) > 0:
                comments = fetch_comments(vk, post_id=post["id"])
                time.sleep(0.34)

            result.append({"post": post, "comments": comments})
            fetched += 1

            if n is not None and fetched >= n:
                return result

        if len(posts) < to_fetch:
            break

        offset += batch
        time.sleep(0.34)

    return result
