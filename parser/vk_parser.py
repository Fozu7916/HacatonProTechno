import time
import os
import vk_api
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

VK_TOKEN = os.getenv("VK_SERVICE_TOKEN") or os.getenv("VK_TOKEN")
GROUP_ID = os.getenv("GROUP_ID")  # числовой ID группы (без минуса)


def get_group_ids() -> list[str]:
    """Возвращает список ID групп из GROUP_IDS или GROUP_ID."""
    raw_ids = os.getenv("GROUP_IDS", "").strip()
    if raw_ids:
        return [gid.strip().replace("-", "") for gid in raw_ids.split(",") if gid.strip()]
    single = str(os.getenv("GROUP_ID", "")).strip().replace("-", "")
    return [single] if single else []


def get_vk_session() -> vk_api.VkApi:
    session = vk_api.VkApi(token=VK_TOKEN)
    return session


def fetch_posts(vk, group_id: str, count: int = 100, offset: int = 0) -> list[dict]:
    """
    Получает посты из сообщества.
    count  — сколько постов за один запрос (макс. 100)
    offset — смещение (для пагинации)
    """
    response = vk.wall.get(
        owner_id=f"-{group_id}",
        count=count,
        offset=offset,
        extended=0,
    )
    return response.get("items", [])


def fetch_comments(vk, group_id: str, post_id: int, count: int = 100) -> list[dict]:
    """Получает комментарии к посту."""
    all_comments = []
    offset = 0

    while True:
        response = vk.wall.getComments(
            owner_id=f"-{group_id}",
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

def upload_photo(vk, photo_file, group_id: str = None):
    """Загружает фото на сервера VK и возвращает строку вложения."""
    import requests
    gids = get_group_ids()
    gid = (group_id or (gids[0] if gids else "")).strip().replace("-", "")
    
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


def upload_doc(vk, file_obj, filename: str = "file.bin", group_id: str = None):
    """Загружает документ/видео (как doc) на сервера VK и возвращает attachment."""
    import requests
    try:
        kwargs = {}
        if group_id:
            kwargs["group_id"] = str(group_id).strip().replace("-", "")
        upload_server = vk.docs.getWallUploadServer(**kwargs)
        upload_url = upload_server["upload_url"]
        files = {"file": (filename, file_obj)}
        response = requests.post(upload_url, files=files).json()
        save_res = vk.docs.save(file=response["file"], title=filename)
        doc = save_res["doc"]
        return f"doc{doc['owner_id']}_{doc['id']}"
    except Exception as e:
        print(f"[VK Doc Upload Error] {e}")
        raise e


def upload_attachment(vk, file_obj, filename: str, group_id: str = None):
    """
    Универсальная загрузка вложения:
    - изображения -> photo
    - остальные файлы (видео/документы) -> doc
    """
    lower = filename.lower()
    if lower.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
        return upload_photo(vk, file_obj, group_id=group_id)
    return upload_doc(vk, file_obj, filename=filename, group_id=group_id)


def parse_all_posts(n: int = None, group_ids: list[str] = None) -> list[dict]:
    """
    Парсит все (или последние n) постов сообщества вместе с комментариями.
    Возвращает список словарей: {"post": ..., "comments": [...]}
    """
    session = get_vk_session()
    vk = session.get_api()

    result = []
    target_group_ids = group_ids or get_group_ids()
    if not target_group_ids:
        return result

    batch = 100
    per_group_limit = n if n is not None else None

    for group_id in target_group_ids:
        offset = 0
        fetched = 0
        while True:
            to_fetch = batch if per_group_limit is None else min(batch, per_group_limit - fetched)
            if to_fetch <= 0:
                break
            try:
                posts = fetch_posts(vk, group_id=group_id, count=to_fetch, offset=offset)
            except Exception as e:
                print(f"[VK Parser] Пропуск группы {group_id}: {e}")
                break

            if not posts:
                break

            for post in posts:
                comments = []
                if post.get("comments", {}).get("count", 0) > 0:
                    comments = fetch_comments(vk, group_id=group_id, post_id=post["id"])
                    time.sleep(0.34)

                result.append({"post": post, "comments": comments})
                fetched += 1

                if per_group_limit is not None and fetched >= per_group_limit:
                    break

            if per_group_limit is not None and fetched >= per_group_limit:
                break
            if len(posts) < to_fetch:
                break

            offset += len(posts)
            time.sleep(0.34)

    return result
