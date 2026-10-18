# HacatonProTechno

**Content analysis and publishing system.** HacatonProTechno automatically collects, analyzes and publishes content from social networks (VKontakte, Telegram): it monitors activity in the specified groups, builds detailed statistics and distributes processed, prioritized content to users or target groups.

## ✨ Key Features

- **Multi-platform parsing:** collects data from VKontakte and Telegram through dedicated modules.
- **Deep content analysis:** calculates content priority, processes incoming posts, builds user and group statistics and generates PDF reports.
- **Automatic publishing:** schedules and distributes filtered, prepared content to target groups.
- **Historical data management:** supports statistics over past periods (e.g. the last 7 days).

## 🚀 Quick Start

```bash
pip install -r requirements.txt
```

Set your API tokens and group IDs (see [Configuration](DOCUMENTATION.md#3-configuration)), then run:

```bash
python parser/main.py        # 1. collect data
python analytics/processor.py # 2. analyze
python publisher/worker.py   # 3. start scheduled publishing
```

## 📚 Documentation

Architecture, modules, configuration and the full startup guide: [DOCUMENTATION.md](DOCUMENTATION.md)
