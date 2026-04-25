# VK Community Parser (HacatonProTechno)

Парсер постов и комментариев из сообществ VK с сохранением в MySQL.

## Установка

1. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
2. Создайте базу данных MySQL:
   ```sql
   CREATE DATABASE vk_parser CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
   ```

### Собрать N последних постов
Например, для получения последних 50 постов:
```bash
python parser/main.py --count 50
```
 или
```bash
python parser/main.py -c 50
```