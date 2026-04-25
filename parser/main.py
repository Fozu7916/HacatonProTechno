import argparse
from db import init_db, upsert_post, upsert_comment
from vk_parser import parse_all_posts


def main():
    parser = argparse.ArgumentParser(description="VK Community Parser")
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Количество последних постов для парсинга (по умолчанию — все)",
    )
    args = parser.parse_args()

    print("[*] Инициализация БД...")
    init_db()

    print(f"[*] Начинаем парсинг{'...' if args.count is None else f' последних {args.count} постов...'}")
    data = parse_all_posts(n=args.count)

    print(f"[*] Получено постов: {len(data)}. Записываем в БД...")
    for entry in data:
        post = entry["post"]
        upsert_post(post)

        for comment in entry["comments"]:
            upsert_comment(comment, post_id=post["id"])

    print(f"[+] Готово. Записано постов: {len(data)}")


if __name__ == "__main__":
    main()
