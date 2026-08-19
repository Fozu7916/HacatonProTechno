# HacatonProTechno - Content Analysis and Publishing System

## 🚀 Project Description

HacatonProTechno is a comprehensive system for automatically collecting, analyzing, and publishing content from various social networks (VKontakte, Telegram). The project is designed to monitor activity in specified groups, generate detailed statistics, and distribute processed, prioritized content to users or target groups.

The system combines data parsing, advanced analytics, and scheduled posting.

## ✨ Key Features

* **Multi-Platform Parsing:** Collects data from VKontakte and Telegram through specialized modules.
* **Deep Content Analysis (Analytics):** Calculates content priority, processes incoming posts (`processor.py`), obtains detailed user and group statistics, and generates PDF reports.
* **Automatic Publishing:** Scheduling and distributing filtered and prepared content to target groups via the `publisher` modules.
* **Historical Data Management:** Support for complex logic for working with historical data (e.g., calculating statistics for the last 7 days).

## 📂 Project Architecture

The project is divided into three key functional blocks, ensuring a transparent and modular data processing process:

1. **`parser/` (Data Parsing):**
* Responsible for extracting raw data from external sources (VK, TG).
* Includes specialized parsers (`telegram_parser`, `vk_parser`) for working with the API and retrieving posts, comments, photos, and documents.

2. **`analytics/` (Analytics):**
* The core of the business logic. Receives raw data from the `parser`. * Calculates metrics (`calculate_priority`), processes content (`process_incoming_post`).
* Collects and provides statistics, and generates reports (PDF).

3. **`publisher/` (Publishing):**
* Responsible for distributing processed content.
* The `worker` module runs background tasks on a schedule (`scheduler`), sending scheduled messages and files to target groups on Telegram and VKontakte.

## ⚙️ Installation and Running

### Prerequisites
Make sure you have the necessary Python libraries installed:
```bash
pip install -r requirements.txt
```

### Configuration
API tokens and group IDs should be configured in the appropriate files (e.g., `config/settings.py`, if used).

### System Startup

1. **Start Parsing:** To extract data, we recommend running a script that will call parsers to gather fresh information:
```bash
python parser/main.py
```
2. **Perform Analysis and Report Generation:** After collecting data, you can start processing and generating statistics:
```bash
# Example of calling the processing process depending on business logic
python analytics/processor.py
# Or to generate a report:
# python analytics/stats.py
```
3. **Start Publishing (In Worker Mode):** To activate scheduled mailings and publications, start a background worker process:
```bash
python publisher/worker.py
```

## 📚 Modules and Files

* `app.py`: The main file for launching the application (entry point).
* `database/db.py`: Manages the connection to the database.
