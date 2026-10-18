# HacatonProTechno Documentation

- [1. Overview](#1-overview)
- [2. Architecture](#2-architecture)
- [3. Configuration](#3-configuration)
- [4. Installation and Running](#4-installation-and-running)
- [5. Modules and Files](#5-modules-and-files)

---

## 1. Overview

HacatonProTechno is a system for automatically collecting, analyzing and publishing content from VKontakte and Telegram. It combines data parsing, analytics and scheduled posting:

- monitors activity in the specified groups;
- generates detailed statistics (users, groups, historical periods such as the last 7 days);
- distributes processed, prioritized content to users or target groups.

---

## 2. Architecture

The project is split into three functional blocks, which keeps data processing transparent and modular.

```
parser/  →  analytics/  →  publisher/
(collect)   (analyze)      (publish)
```

### 2.1 `parser/` — Data Parsing

- Extracts raw data from external sources (VK, Telegram).
- Contains specialized parsers (`telegram_parser`, `vk_parser`) that work with the platform APIs and retrieve posts, comments, photos and documents.

### 2.2 `analytics/` — Analytics

The core of the business logic. It receives raw data from `parser/` and:

- calculates metrics (`calculate_priority`);
- processes incoming content (`process_incoming_post`, in `processor.py`);
- collects and provides statistics;
- generates PDF reports.

### 2.3 `publisher/` — Publishing

- Distributes processed content.
- The `worker` module runs background tasks on a schedule (`scheduler`), sending scheduled messages and files to target groups on Telegram and VKontakte.

---

## 3. Configuration

API tokens and group IDs are set in the corresponding configuration files (for example `config/settings.py`, if used in your setup).

---

## 4. Installation and Running

### 4.1 Prerequisites

Install the required Python libraries:

```bash
pip install -r requirements.txt
```

### 4.2 Startup

1. **Parsing.** Run the script that calls the parsers to gather fresh data:

```bash
   python parser/main.py
```

2. **Analysis and report generation.** After the data is collected, start processing and statistics:

```bash
   # Run processing (depends on your business logic)
   python analytics/processor.py

   # Or generate a report:
   # python analytics/stats.py
```

3. **Publishing (worker mode).** Start the background worker to activate scheduled mailings and publications:

```bash
   python publisher/worker.py
```

---

## 5. Modules and Files

| File / Module        | Purpose                                          |
|----------------------|--------------------------------------------------|
| `app.py`             | Main application entry point                     |
| `database/db.py`     | Database connection management                   |
| `parser/main.py`     | Runs the VK and Telegram parsers                 |
| `analytics/processor.py` | Processing of incoming posts                 |
| `analytics/stats.py` | Statistics and report generation                 |
| `publisher/worker.py`| Background worker with scheduled publishing      |
