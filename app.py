import streamlit as st
import pandas as pd
import os
import time
import requests
import io
from datetime import datetime
from streamlit_calendar import calendar
from custom_calendar import queue_calendar_popup_component
from database.db import (
    get_connection,
    init_db,
    get_setting,
    set_setting,
    add_template,
    get_templates,
    add_to_queue,
    create_user,
    authenticate_user,
    update_user_credentials,
    create_pending_registration,
    get_pending_registrations,
    approve_registration,
    reject_registration,
)
from analytics.stats import get_dry_stats
from analytics.processor import process_incoming_post
from publisher.scheduler import publish_next_post
st.set_page_config(page_title="VK Volunteer Panel", layout="wide")

st.title("📱 Панель управления контентом")

ATTACHMENT_TYPES = [
    "png", "jpg", "jpeg", "webp", "gif",
    "mp4", "mov", "avi", "mkv",
    "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "zip", "rar",
]


def generate_text_with_ai(user_prompt: str, role_hint: str = "SMM-редактор") -> str:
    """
    Генерирует текст через OpenRouter/OpenAI-совместимый API.
    Для работы нужен OPENROUTER_API_KEY или OPENAI_API_KEY в .env.
    """
    api_key = os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("Не найден API-ключ (OPENROUTER_API_KEY или OPENAI_API_KEY в .env)")

    base_url = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
    model = os.getenv("LLM_MODEL", "deepseek/deepseek-chat-v3-0324:free")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    # Для OpenRouter полезно передавать origin приложения.
    if "openrouter.ai" in base_url:
        headers["HTTP-Referer"] = os.getenv("APP_URL", "http://localhost:8501")
        headers["X-Title"] = os.getenv("APP_NAME", "HacatonProTechno")

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Ты опытный редактор соцсетей молодёжного центра. "
                    "Пиши живо, структурно, без канцелярита, на русском языке. "
                    "Добавляй уместные эмодзи (2-6 штук по тексту), без перегруза."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Роль: {role_hint}\n"
                    "Сгенерируй готовый пост для VK на основе данных ниже. "
                    "Сохрани факты, добавь цепляющий заголовок, короткий призыв к действию и 3-6 хештегов.\n\n"
                    f"{user_prompt}"
                ),
            },
        ],
        "temperature": 0.7,
    }
    endpoints = [f"{base_url.rstrip('/')}/chat/completions"]
    # Fallback для случаев, когда в LLM_BASE_URL указан только домен.
    if "/api/v1" not in base_url:
        endpoints.append(f"{base_url.rstrip('/')}/api/v1/chat/completions")

    # Fallback модели: у некоторых аккаунтов :free недоступен.
    model_candidates = [model]
    if model.endswith(":free"):
        model_candidates.append(model.replace(":free", ""))
    if "deepseek" in model:
        model_candidates.append("deepseek/deepseek-chat")

    last_error = None
    for candidate_model in model_candidates:
        payload["model"] = candidate_model
        for url in endpoints:
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=45)
                if response.ok:
                    data = response.json()
                    text = data["choices"][0]["message"]["content"].strip()
                    if not text:
                        raise ValueError("ИИ вернул пустой ответ")
                    return text

                err_text = response.text[:500]
                last_error = f"{response.status_code} for {url}: {err_text}"
            except requests.RequestException as e:
                last_error = str(e)

    raise ValueError(f"Не удалось сгенерировать текст через ИИ: {last_error}")


def build_excel_report(report_data: dict) -> bytes:
    """Собирает Excel-отчет по данным аналитики."""
    output = io.BytesIO()
    chart_df = report_data.get("chart_data")
    daily_df = report_data.get("daily_stats")
    top3_df = pd.DataFrame(report_data.get("top_3", []))
    stats_df = pd.DataFrame(
        [{"Показатель": k, "Значение": v} for k, v in report_data.get("stats", {}).items()]
    )

    try:
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            stats_df.to_excel(writer, index=False, sheet_name="KPI")
            rows = 0
            if isinstance(chart_df, pd.DataFrame) and not chart_df.empty:
                chart_df.reset_index().to_excel(writer, index=False, sheet_name="Динамика")
            if isinstance(daily_df, pd.DataFrame) and not daily_df.empty:
                daily_df.reset_index().to_excel(writer, index=False, sheet_name="По дням")
            if not top3_df.empty:
                top3_df.to_excel(writer, index=False, sheet_name="Топ посты")
            # Лист с графиками и сводкой (как в дашборде)
            workbook = writer.book
            ws = workbook.add_worksheet("Графики")
            writer.sheets["Графики"] = ws
            ws.write("A1", "Дашборд графики")

            if isinstance(chart_df, pd.DataFrame) and not chart_df.empty:
                dyn = chart_df.reset_index().copy()
                dyn["date"] = dyn["date"].astype(str)
                dyn.to_excel(writer, index=False, sheet_name="Графики", startrow=2, startcol=0)
                rows = len(dyn)
                line_chart = workbook.add_chart({"type": "line"})
                line_chart.add_series({
                    "name": "Просмотры",
                    "categories": ["Графики", 3, 0, rows + 2, 0],
                    "values": ["Графики", 3, 1, rows + 2, 1],
                })
                line_chart.add_series({
                    "name": "Лайки",
                    "categories": ["Графики", 3, 0, rows + 2, 0],
                    "values": ["Графики", 3, 2, rows + 2, 2],
                })
                line_chart.set_title({"name": "Динамика охвата и лайков"})
                line_chart.set_legend({"position": "bottom"})
                ws.insert_chart("E3", line_chart, {"x_scale": 1.4, "y_scale": 1.3})

            if isinstance(daily_df, pd.DataFrame) and not daily_df.empty:
                summary = {
                    "Показатель": ["Просмотры", "Лайки", "Репосты", "Комментарии"],
                    "Значение": [
                        int(daily_df["views"].sum()),
                        int(daily_df["likes"].sum()),
                        int(daily_df["reposts"].sum()),
                        int(daily_df["comments"].sum()),
                    ],
                }
                s_df = pd.DataFrame(summary)
                s_df.to_excel(writer, index=False, sheet_name="Графики", startrow=rows + 6 if isinstance(chart_df, pd.DataFrame) and not chart_df.empty else 2, startcol=0)
                s_start = (rows + 7) if isinstance(chart_df, pd.DataFrame) and not chart_df.empty else 3
                s_end = s_start + len(s_df) - 1
                bar_chart = workbook.add_chart({"type": "column"})
                bar_chart.add_series({
                    "name": "Суммарные метрики",
                    "categories": ["Графики", s_start, 0, s_end, 0],
                    "values": ["Графики", s_start, 1, s_end, 1],
                })
                bar_chart.set_title({"name": "Суммарные метрики за период"})
                ws.insert_chart("E22", bar_chart, {"x_scale": 1.2, "y_scale": 1.2})
    except Exception as e:
        raise ValueError(f"Ошибка генерации Excel: {e}")

    return output.getvalue()


def render_emoji_toolbar(target_key: str, toolbar_key: str):
    """Emoji picker в выпадающем окне с категориями."""
    emoji_groups = {
        "Популярные": ["🔥", "✨", "🎉", "📢", "📅", "📍", "💙", "✅", "🚀", "🤝", "💡", "🎓", "❤️", "👏", "🙏", "🌟"],
        "Эмоции": ["😀", "😁", "😂", "🤣", "😊", "😍", "😘", "😎", "🤗", "🤔", "😴", "😭", "😡", "🥳", "🤩", "😇"],
        "События": ["🎊", "🎈", "🎁", "🎤", "🎬", "🎵", "🏆", "🥇", "📸", "🎯", "🧩", "🎨", "🎭", "🎪", "🏅", "🎟️"],
        "Люди": ["👩‍💻", "🧑‍💻", "👨‍🏫", "🧑‍🎓", "🙌", "👏", "🤝", "💬", "🫶", "🧠", "💪", "🫡", "👥", "🗣️", "🧑‍🤝‍🧑", "✍️"],
        "Объявления": ["📢", "📣", "📰", "🗓️", "🕒", "📌", "📝", "📍", "🔔", "⚡", "❗", "✅", "📎", "🔗", "📊", "📈"],
        "Природа": ["🌞", "🌈", "🌿", "🌸", "🍀", "🌍", "🌊", "🌙", "⭐", "☀️", "🍁", "🌱", "🌼", "🌻", "🌺", "❄️"],
    }

    with st.popover("😊 Эмодзи"):
        tabs = st.tabs(list(emoji_groups.keys()))
        for tab_name, tab in zip(emoji_groups.keys(), tabs):
            with tab:
                emojis = emoji_groups[tab_name]
                cols = st.columns(8)
                for i, emj in enumerate(emojis):
                    with cols[i % 8]:
                        if st.button(emj, key=f"{toolbar_key}_{tab_name}_{i}"):
                            st.session_state[f"{toolbar_key}__pending_emoji"] = emj
                            st.rerun()


def apply_pending_emoji(target_key: str, toolbar_key: str):
    """
    Безопасно добавляет эмодзи к тексту ДО создания text_area с этим key.
    Это обходит ограничение Streamlit на изменение state после инстанцирования виджета.
    """
    pending_key = f"{toolbar_key}__pending_emoji"
    if pending_key in st.session_state and st.session_state[pending_key]:
        current_text = st.session_state.get(target_key, "")
        st.session_state[target_key] = f"{current_text}{st.session_state[pending_key]}"
        st.session_state[pending_key] = ""

# Инициализация БД при первом запуске
if st.sidebar.button("Инициализировать БД"):
    from database.db import init_db
    import importlib
    import database.db
    importlib.reload(database.db) # Принудительно обновляем модуль в памяти
    database.db.init_db()
    st.sidebar.success("Таблицы созданы и обновлены!")
    st.rerun()

# --- БОКОВАЯ ПАНЕЛЬ ---
st.sidebar.header("👤 Авторизация")
if "auth_user" not in st.session_state:
    st.session_state["auth_user"] = None

if st.session_state["auth_user"] is None:
    auth_mode = st.sidebar.radio("Режим", ["Вход", "Регистрация"], horizontal=True)
    if auth_mode == "Вход":
        email_in = st.sidebar.text_input("Почта", key="login_email")
        pass_in = st.sidebar.text_input("Пароль", type="password", key="login_password")
        if st.sidebar.button("Войти"):
            user = authenticate_user(email_in.strip(), pass_in)
            if user:
                st.session_state["auth_user"] = user
                st.sidebar.success(f"Добро пожаловать, {user['full_name']} ({user['code']})")
                st.rerun()
            else:
                st.sidebar.error("Неверная почта или пароль")
    else:
        reg_name = st.sidebar.text_input("ФИО", key="reg_full_name")
        reg_email = st.sidebar.text_input("Почта", key="reg_email")
        reg_password = st.sidebar.text_input("Пароль", type="password", key="reg_password")
        reg_role = st.sidebar.selectbox(
            "Роль",
            ["Наблюдатель", "Руководитель", "Администратор", "Редактор", "Волонтер", "СММ"],
            key="reg_role",
        )
        if st.sidebar.button("Зарегистрироваться"):
            try:
                create_pending_registration(reg_name.strip(), reg_email.strip(), reg_password, reg_role)
                st.sidebar.success("✅ Заявка отправлена! Ожидайте одобрения администратора.")
            except Exception as e:
                st.sidebar.error(f"Ошибка регистрации: {e}")
    st.warning("Войдите в систему для работы с платформой.")
    st.stop()

auth_user = st.session_state["auth_user"]
role = auth_user["role"]
if role in ["СММ", "СММ-специалист"]:
    role = "СММ"
user_code = auth_user["code"]
st.sidebar.caption(f"Пользователь: {auth_user['full_name']} ({user_code})")

# Кнопка для изменения логина/пароля
if st.sidebar.button("🔐 Изменить логин/пароль"):
    st.session_state["show_credentials_modal"] = True

# Модальное окно для изменения учетных данных
if st.session_state.get("show_credentials_modal", False):
    with st.sidebar.expander("🔐 Изменить учетные данные", expanded=True):
        st.warning("⚠️ Оставьте поле пустым, если не хотите его менять")
        
        new_email = st.text_input(
            "Новая почта (опционально)",
            value="",
            key="change_email_input",
            placeholder=auth_user["email"]
        )
        
        new_password = st.text_input(
            "Новый пароль (опционально)",
            type="password",
            value="",
            key="change_password_input",
            placeholder="Введите новый пароль"
        )
        
        confirm_password = st.text_input(
            "Подтвердите пароль",
            type="password",
            value="",
            key="confirm_password_input",
            placeholder="Повторите пароль"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Сохранить", key="save_credentials_btn"):
                # Валидация
                if new_password and new_password != confirm_password:
                    st.error("❌ Пароли не совпадают!")
                elif not new_email and not new_password:
                    st.warning("⚠️ Укажите хотя бы одно поле для изменения")
                else:
                    try:
                        update_user_credentials(
                            auth_user["email"],
                            new_password=new_password if new_password else None,
                            new_email=new_email if new_email else None
                        )
                        st.success("✅ Учетные данные обновлены!")
                        # Обновляем session_state если изменена почта
                        if new_email:
                            auth_user["email"] = new_email
                        st.session_state["show_credentials_modal"] = False
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Ошибка: {e}")
        
        with col2:
            if st.button("❌ Отмена", key="cancel_credentials_btn"):
                st.session_state["show_credentials_modal"] = False
                st.rerun()

if st.sidebar.button("Выйти"):
    st.session_state["auth_user"] = None
    st.rerun()
if role in ["Руководитель", "Наблюдатель", "Администратор"]:
    st.sidebar.header("⚙️ Управление")
    # Слайдер теперь от 1 поста
    stat_limit = st.sidebar.slider("Глубина анализа (постов)", 1, 200, 50)

    if st.sidebar.button("🔄 Обновить данные из VK"):
        with st.sidebar.status("Парсинг..."):
            from parser.vk_parser import parse_all_posts
            from database.db import upsert_post
            data = parse_all_posts(n=200)
            for item in data:
                upsert_post(item["post"])
        st.sidebar.success("Обновлено!")
        st.rerun()
    st.sidebar.header("🔑 Панель Руководителя")

    report_days = st.sidebar.number_input("Период отчета (дней)", 1, 365, 7)
    if st.sidebar.button("📈 Сформировать отчет"):
        from analytics.stats import get_stats_by_days
        st.session_state['active_report'] = get_stats_by_days(report_days)
        if st.session_state['active_report']:
            st.toast(f"Отчет за {report_days} дн. готов!")
        else:
            st.sidebar.error("Нет данных за этот период")

    if 'active_report' in st.session_state and st.session_state['active_report']:
        report_data = st.session_state['active_report']
        with st.expander("📋 Расширенный отчет для руководства", expanded=True):
            report = report_data["stats"]

            st.markdown("### 📌 KPI дашборд")
            kpi_cols = st.columns(4)
            kpi_cols[0].metric("Период", f"{report.get('Период (дней)', '-') } дн.")
            kpi_cols[1].metric("Постов", report.get("Всего постов", "-"))
            kpi_cols[2].metric("Охват", report.get("Всего охвата", "-"))
            kpi_cols[3].metric("ER (%)", report.get("Средний ER (%)", "-"))

            info_cols = st.columns(3)
            info_cols[0].info(f"🕐 Лучшие часы: {report.get('Лучшие часы', '-')}")
            info_cols[1].success(f"📅 Лучший день: {report.get('Лучший день недели', '-')}")
            info_cols[2].warning(f"🏷 Темы: {report.get('Ключевые темы', '-')}")

            st.markdown("### 📈 Динамика")
            chart_cols = st.columns(2)
            with chart_cols[0]:
                st.caption("Охват и лайки по датам")
                st.line_chart(report_data["chart_data"], width="stretch")
            with chart_cols[1]:
                st.caption("Суммарные метрики за период")
                summary_df = pd.DataFrame(
                    {
                        "Показатель": ["Просмотры", "Лайки", "Репосты", "Комментарии"],
                        "Значение": [
                            int(report_data["daily_stats"]["views"].sum()),
                            int(report_data["daily_stats"]["likes"].sum()),
                            int(report_data["daily_stats"]["reposts"].sum()),
                            int(report_data["daily_stats"]["comments"].sum()),
                        ],
                    }
                )
                st.bar_chart(summary_df.set_index("Показатель"), width="stretch")

            st.markdown("### 📅 Ежедневная статистика")
            st.dataframe(report_data["daily_stats"], width="stretch")

            st.markdown("### 🏆 Топ-3 лучших поста")
            st.subheader("🏆 Топ-3 лучших поста за период")
            for post in report_data["top_3"]:
                st.markdown(f"**Просмотры: {post['views']} | Лайки: {post['likes']}**")
                st.caption(post['text'])
                st.link_button("Открыть пост", post['link'])
                st.divider()

            export_col1, export_col2 = st.columns(2)
            from analytics.stats import generate_pdf_report
            try:
                pdf_output = generate_pdf_report(report_data, chart_df=report_data["chart_data"])
                pdf_bytes = bytes(pdf_output)
                with export_col1:
                    st.download_button(
                        label="📥 Скачать отчет в PDF",
                        data=pdf_bytes,
                        file_name="report.pdf",
                        mime="application/pdf",
                        width="stretch",
                    )
            except Exception as e:
                st.error(f"Ошибка генерации PDF: {e}")
            try:
                excel_bytes = build_excel_report(report_data)
                with export_col2:
                    st.download_button(
                        label="📊 Скачать отчет в Excel",
                        data=excel_bytes,
                        file_name="report.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        width="stretch",
                    )
            except Exception as e:
                st.error(f"Ошибка генерации Excel: {e}")

    st.sidebar.header("📊 Статистика")
    stats, _ = get_dry_stats(limit=stat_limit)
    if stats:
        for key, val in stats.items():
            st.sidebar.metric(key, val)

# --- ОСНОВНАЯ ОБЛАСТЬ ---
if role == "Наблюдатель":
    tab_list = ["📈 Архив"]
    tabs = st.tabs(tab_list)
    tab_a = tabs[0]
elif role in ["Руководитель"]:
    tab_list = ["📅 Очередь", "📈 Архив"]
    tabs = st.tabs(tab_list)
    tab_q, tab_a = tabs[0], tabs[1]
elif role == "Администратор":
    tab_list = ["� Регистрации", "� Редактор", "📅 Очередь", "📈 Архив"]
    tabs = st.tabs(tab_list)
    tab_reg, tab_r, tab_q, tab_a = tabs[0], tabs[1], tabs[2], tabs[3]
else:
    tab_list = ["📝 Редактор", "📅 Очередь", "📈 Архив"]
    tabs = st.tabs(tab_list)
    tab_r, tab_q, tab_a = tabs[0], tabs[1], tabs[2]

# Логика вкладки Регистрации (только для администратора)
if role == "Администратор":
    with tab_reg:
        st.header("👥 Управление регистрациями")
        
        pending_regs = get_pending_registrations()
        
        if not pending_regs:
            st.info("✅ Нет ожидающих регистраций")
        else:
            st.warning(f"⏳ Ожидающих регистраций: {len(pending_regs)}")
            
            for reg in pending_regs:
                with st.container(border=True):
                    col1, col2, col3 = st.columns([2, 1, 1])
                    
                    with col1:
                        st.markdown(f"**ФИО:** {reg['full_name']}")
                        st.markdown(f"**Email:** {reg['email']}")
                        st.markdown(f"**Роль:** {reg['role']}")
                        st.caption(f"Заявка от: {reg['created_at']}")
                    
                    with col2:
                        if st.button("✅ Одобрить", key=f"approve_{reg['id']}"):
                            try:
                                code = approve_registration(reg['id'], user_code)
                                st.success(f"✅ Пользователь создан: {code}")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Ошибка: {e}")
                    
                    with col3:
                        if st.button("❌ Отклонить", key=f"reject_{reg['id']}"):
                            st.session_state[f"reject_reason_{reg['id']}"] = True
                    
                    # Форма отклонения
                    if st.session_state.get(f"reject_reason_{reg['id']}", False):
                        reason = st.text_area(
                            "Причина отклонения",
                            key=f"reject_reason_text_{reg['id']}",
                            height=80
                        )
                        col_confirm1, col_confirm2 = st.columns(2)
                        with col_confirm1:
                            if st.button("✅ Подтвердить отклонение", key=f"confirm_reject_{reg['id']}"):
                                try:
                                    reject_registration(reg['id'], user_code, reason)
                                    st.success("✅ Заявка отклонена")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ Ошибка: {e}")
                        with col_confirm2:
                            if st.button("❌ Отмена", key=f"cancel_reject_{reg['id']}"):
                                st.session_state[f"reject_reason_{reg['id']}"] = False
                                st.rerun()
                    
                    st.divider()

# Логика вкладки Редактор (только если она есть)
if role not in ["Руководитель", "Наблюдатель"]:
    with tab_r:
        active_creator_role = role
        if role == "Администратор":
            active_creator_role = st.radio(
                "Режим администратора",
                ["СММ", "Редактор"],
                horizontal=True,
            )

        if active_creator_role == "СММ":
            st.header("🚀 Панель СММ")
            
            # Блок рекомендаций на основе аналитики
            from analytics.stats import get_stats_by_days
            with st.expander("💡 Рекомендации по контенту", expanded=True):
                rec_data = get_stats_by_days(30) # Анализ за месяц
                if rec_data:
                    st.success(f"🔥 **Лучшее время для публикации:** {rec_data['stats']['Лучшие часы']}")
                    st.info(f"📅 **Самый активный день:** {rec_data['stats']['Лучший день недели']}")
                    st.write(f"🔍 **Популярные темы:** {rec_data['stats']['Ключевые темы']}")
                else:
                    st.info("Соберите больше данных из VK для получения рекомендаций")

            # Управление количеством постов в день            from database.db import get_setting, set_setting
            current_ppd = int(get_setting('posts_per_day', 3))
            new_ppd = st.number_input("Лимит постов в день (план)", 1, 24, current_ppd)
            if new_ppd != current_ppd:
                set_setting('posts_per_day', new_ppd)
                st.success(f"Лимит постов обновлен до {new_ppd}")
            
            st.divider()
            
            smm_mode = st.radio("Режим создания", ["Свободный текст", "Создание афиши по шаблону"])
            
            if smm_mode == "Свободный текст":
                apply_pending_emoji("smm_text", "emoji_smm_free")
                smm_text = st.text_area("Текст официального поста", height=150, key="smm_text")
                render_emoji_toolbar("smm_text", "emoji_smm_free")
                if st.button("🤖 Сгенерировать текст ИИ (смайлики)"):
                    try:
                        ai_prompt = f"Напиши пост для VK на тему:\n{smm_text or 'Анонс мероприятия молодёжного центра'}"
                        ai_text = generate_text_with_ai(ai_prompt, role_hint="СММ")
                        st.session_state["smm_text"] = ai_text
                        st.success("ИИ-текст сгенерирован")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Ошибка ИИ-генерации: {e}")
                smm_uploaded_file = st.file_uploader(
                    "Прикрепите вложения (фото/видео/документы)",
                    type=ATTACHMENT_TYPES,
                    accept_multiple_files=True,
                    key="smm_photo_free"
                )
            else:
                from database.db import get_templates
                templates = get_templates()
                if templates:
                    t_names = [t['name'] for t in templates]
                    sel_t = st.selectbox("Выберите шаблон афиши", t_names, key="smm_tpl_sel")
                    template_obj = next(t for t in templates if t['name'] == sel_t)
                    template_content = template_obj['content']
                    
                    st.info(f"📋 **Структура шаблона:**\n\n{template_content}")
                    
                    # Динамические поля для СММ
                    import re
                    tags = re.findall(r"\{(.*?)\}", template_content)
                    smm_inputs = {}
                    if tags:
                        st.subheader("Заполните данные:")
                        cols = st.columns(2)
                        for i, tag in enumerate(tags):
                            with cols[i % 2]:
                                smm_inputs[tag] = st.text_input(f"Введите {tag}", key=f"smm_tag_{tag}")
                    
                    if st.button("🤖 Сгенерировать ИИ-версию афиши"):
                        draft_text = template_content
                        ai_prompt = (
                            f"Название шаблона: {sel_t}\n"
                            f"Текст шаблона:\n{template_content}\n\n"
                            "Заполненные поля:\n"
                        )
                        for tag, val in smm_inputs.items():
                            draft_text = draft_text.replace(f"{{{tag}}}", val)
                            ai_prompt += f"- {tag}: {val}\n"
                        ai_prompt += f"\nЧерновик по шаблону:\n{draft_text}"
                        try:
                            ai_text = generate_text_with_ai(ai_prompt, role_hint="СММ")
                            st.session_state['generated_smm_text'] = ai_text
                            st.session_state['smm_text_afisha_area'] = ai_text
                            st.success("ИИ-текст сгенерирован")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Ошибка ИИ-генерации: {e}")
                    
                    if "smm_text_afisha_area" not in st.session_state:
                        st.session_state["smm_text_afisha_area"] = st.session_state.get("generated_smm_text", "")
                    apply_pending_emoji("smm_text_afisha_area", "emoji_smm_afisha")
                    smm_text = st.text_area(
                        "Текст афиши (на основе шаблона)",
                        height=150,
                        key="smm_text_afisha_area"
                    )
                    render_emoji_toolbar("smm_text_afisha_area", "emoji_smm_afisha")
                    smm_uploaded_file = st.file_uploader(
                        "Прикрепите вложения афиши (фото/видео/документы)",
                        type=ATTACHMENT_TYPES,
                        accept_multiple_files=True,
                        key="smm_photo_afisha"
                    )
                else:
                    st.warning("Нет доступных шаблонов.")
                    smm_text = ""
                    smm_uploaded_file = []

            col_date, col_hour, col_min, col_btn = st.columns([1.5, 1, 1, 1.5])
            with col_date:
                d = st.date_input("Дата", datetime.now())
            with col_hour:
                h = st.selectbox("Час", range(24), index=datetime.now().hour)
            with col_min:
                m = st.selectbox("Мин", range(0, 60, 5), index=0)
                
            scheduled_at = datetime.combine(d, datetime.min.time()).replace(hour=h, minute=m)
            
            with col_btn:
                st.write(" ")
                st.write(" ")
                if st.button("В очередь"):
                    if smm_text:
                        smm_attachments = None
                        if smm_uploaded_file:
                            try:
                                if not os.path.exists("uploads"):
                                    os.makedirs("uploads")
                                files_saved = []
                                for fobj in smm_uploaded_file:
                                    file_path = os.path.join("uploads", f"{int(time.time())}_{fobj.name}")
                                    with open(file_path, "wb") as fw:
                                        fw.write(fobj.getbuffer())
                                    files_saved.append(file_path)
                                smm_attachments = ",".join(files_saved) if files_saved else None
                            except Exception as e:
                                st.error(f"Ошибка подготовки вложений: {e}")
                        
                        from database.db import add_to_queue
                        smm_title = smm_text.splitlines()[0][:255] if smm_text else "Без заголовка"
                        add_to_queue(
                            None,
                            smm_text,
                            priority=5,
                            er=0.0,
                            scheduled_at=scheduled_at,
                            attachments=smm_attachments,
                            title=smm_title,
                            author_code=user_code,
                            approver_code=user_code,
                        )
                        
                        # СММ посты сразу получают статус 'ready', им не нужно одобрение редактора
                        conn = get_connection(); cur = conn.cursor()
                        cur.execute(
                            "UPDATE post_queue SET status = 'ready', author_role = 'smm', author_code = %s, approver_code = %s WHERE suggested_text = %s ORDER BY created_at DESC LIMIT 1",
                            (user_code, user_code, smm_text),
                        )
                        conn.commit(); cur.close(); conn.close()
                        
                        st.success(f"Запланировано на {scheduled_at.strftime('%d.%m %H:%M')} ✅")
                    else:
                        st.error("Текст пуст!")

        elif active_creator_role == "Редактор":
            st.header("🛠 Управление контентом (Редактор)")

            # 0. Создание поста редактором (новое требование)
            st.subheader("✍️ Создать пост (Редактор)")
            editor_title = st.text_input("Заголовок публикации", key="editor_post_title")
            editor_text = st.text_area("Текст публикации", key="editor_post_text", height=130)
            editor_files = st.file_uploader(
                "Вложения (фото/видео/документы)",
                type=ATTACHMENT_TYPES,
                accept_multiple_files=True,
                key="editor_post_files",
            )
            if st.button("Опубликовать/в очередь от редактора"):
                if editor_text.strip():
                    attachments = None
                    if editor_files:
                        if not os.path.exists("uploads"):
                            os.makedirs("uploads")
                        saved = []
                        for fobj in editor_files:
                            fpath = os.path.join("uploads", f"{int(time.time())}_{fobj.name}")
                            with open(fpath, "wb") as fw:
                                fw.write(fobj.getbuffer())
                            saved.append(fpath)
                        attachments = ",".join(saved) if saved else None
                    add_to_queue(
                        None,
                        editor_text,
                        priority=4,
                        er=0.0,
                        attachments=attachments,
                        title=editor_title or (editor_text.splitlines()[0][:255] if editor_text else "Без заголовка"),
                        author_code=user_code,
                        approver_code=user_code,
                    )
                    conn = get_connection(); cur = conn.cursor()
                    cur.execute(
                        "UPDATE post_queue SET status='ready', author_role='smm' WHERE id = LAST_INSERT_ID()"
                    )
                    conn.commit(); cur.close(); conn.close()
                    st.success("Пост редактора добавлен в ready")
                    st.rerun()
                else:
                    st.error("Введите текст публикации")

            st.divider()
            
            # 1. Создание шаблонов
            st.subheader("📝 Создание шаблонов")
            t_name = st.text_input("Название шаблона")
            t_cont = st.text_area("Структура шаблона ({теги})", placeholder="Заголовок: {title}\n...")
            if st.button("Сохранить шаблон"):
                from database.db import add_template
                add_template(t_name, t_cont)
                st.success("Шаблон добавлен!")

            st.divider()
            
            # 2. Проверка постов волонтеров
            st.subheader("⚖️ Проверка предложенных постов")
            conn = get_connection()
            to_edit = pd.read_sql("SELECT * FROM post_queue WHERE status = 'editing' OR (author_role='volunteer' AND status='pending')", conn)
            conn.close()
            
            if not to_edit.empty:
                editor_options = []
                for _, row in to_edit.iterrows():
                    title = str(row.get("title") or "").strip() or str(row.get("suggested_text") or "").splitlines()[0][:80]
                    if len(title) > 80:
                        title = title[:80] + "..."
                    editor_options.append((int(row["id"]), title or "Без заголовка"))
                post_to_fix = st.selectbox(
                    "Выберите пост для проверки",
                    options=[i for i, _ in editor_options],
                    format_func=lambda pid: next((t for i, t in editor_options if i == pid), f"Пост #{pid}"),
                )
                current_row = to_edit[to_edit['id'] == post_to_fix].iloc[0]

                st.info(f"Заголовок: {next((t for i, t in editor_options if i == post_to_fix), 'Без заголовка')}")
                with st.expander("Показать полный текст поста"):
                    st.write(current_row['suggested_text'] or "Без текста")
                if current_row['attachments']:
                    attachments_items = [a.strip() for a in str(current_row['attachments']).split(",") if a.strip()]
                    st.write("Вложения:")
                    for att in attachments_items:
                        st.code(att)
                        if os.path.exists(att):
                            if att.lower().endswith((".png", ".jpg", ".jpeg", ".webp", ".gif")):
                                st.image(att, caption="Предпросмотр (локально)", width=400)
                            else:
                                st.caption("Локальный файл (не изображение)")
                        elif att.startswith("photo"):
                            try:
                                from parser.vk_parser import get_photo_url
                                import vk_api
                                vk = vk_api.VkApi(token=os.getenv("VK_USER_TOKEN")).get_api()
                                img_url = get_photo_url(vk, att)
                                if img_url:
                                    st.image(img_url, caption="Прикрепленное фото", width=400)
                            except Exception:
                                pass
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Одобрить (в очередь)"):
                        conn = get_connection(); cur = conn.cursor()
                        cur.execute(
                            "UPDATE post_queue SET status = 'ready', priority = 3, approver_code = %s WHERE id = %s",
                            (user_code, post_to_fix),
                        )
                        conn.commit(); cur.close(); conn.close()
                        st.success("Пост одобрен!"); st.rerun()
                with col2:
                    if st.button("❌ Отклонить (удалить)"):
                        conn = get_connection(); cur = conn.cursor()
                        cur.execute("DELETE FROM post_queue WHERE id = %s", (post_to_fix,))
                        conn.commit(); cur.close(); conn.close()
                        st.warning("Пост удален!"); st.rerun()
            else:
                st.write("Нет постов на проверку.")
        elif role == "Волонтер":
            st.header("✍️ Предложение поста (Волонтер)")
            from database.db import get_templates
            templates = get_templates()
            
            if templates:
                t_names = [t['name'] for t in templates]
                sel_t = st.selectbox("Выберите тип поста", t_names)
                template_obj = next(t for t in templates if t['name'] == sel_t)
                template_content = template_obj['content']
                
                st.info(f"📋 **Структура шаблона:**\n\n{template_content}")
                
                # Динамическая генерация полей на основе тегов в фигурных скобках
                import re
                tags = re.findall(r"\{(.*?)\}", template_content)
                
                inputs = {}
                if tags:
                    st.subheader("Заполните данные для автогенерации:")
                    cols = st.columns(2)
                    for i, tag in enumerate(tags):
                        with cols[i % 2]:
                            inputs[tag] = st.text_input(f"Введите {tag}", key=f"tag_{tag}")
                
                if st.button("🤖 Сгенерировать текст ИИ"):
                    draft_text = template_content
                    ai_prompt = (
                        f"Название шаблона: {sel_t}\n"
                        f"Текст шаблона:\n{template_content}\n\n"
                        "Заполненные поля:\n"
                    )
                    for tag, val in inputs.items():
                        draft_text = draft_text.replace(f"{{{tag}}}", val)
                        ai_prompt += f"- {tag}: {val}\n"
                    ai_prompt += f"\nЧерновик по шаблону:\n{draft_text}"
                    try:
                        ai_text = generate_text_with_ai(ai_prompt, role_hint="Волонтер")
                        st.session_state['generated_vol_text'] = ai_text
                        st.session_state['vol_text_area_final'] = ai_text
                        st.success("ИИ-текст сгенерирован")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Ошибка ИИ-генерации: {e}")

                if "vol_text_area_final" not in st.session_state:
                    st.session_state["vol_text_area_final"] = st.session_state.get("generated_vol_text", "")
                apply_pending_emoji("vol_text_area_final", "emoji_vol_final")
                final_text = st.text_area(
                    "Итоговый текст (можно подправить)",
                    height=200,
                    key="vol_text_area_final"
                )
                render_emoji_toolbar("vol_text_area_final", "emoji_vol_final")
                uploaded_file = st.file_uploader(
                    "Прикрепите вложения (фото/видео/документы)",
                    type=ATTACHMENT_TYPES,
                    accept_multiple_files=True
                )
                
                if st.button("Отправить на проверку"):
                    if final_text:
                        attachments = None
                        if uploaded_file:
                            # Сохраняем локально для предпросмотра
                            import os
                            if not os.path.exists("uploads"): os.makedirs("uploads")
                            files_saved = []
                            for fobj in uploaded_file:
                                file_path = os.path.join("uploads", f"{int(time.time())}_{fobj.name}")
                                with open(file_path, "wb") as f:
                                    f.write(fobj.getbuffer())
                                files_saved.append(file_path)
                            attachments = ",".join(files_saved) if files_saved else None
                        
                        from database.db import add_to_queue
                        vol_title = final_text.splitlines()[0][:255] if final_text else "Без заголовка"
                        add_to_queue(
                            None,
                            final_text,
                            priority=2,
                            er=0.0,
                            attachments=attachments,
                            title=vol_title,
                            author_code=user_code,
                        )
                        
                        conn = get_connection(); cur = conn.cursor()
                        cur.execute("UPDATE post_queue SET status = 'editing' WHERE suggested_text = %s ORDER BY created_at DESC LIMIT 1", (final_text,))
                        conn.commit(); cur.close(); conn.close()
                        st.success("Пост и фото отправлены редактору!")
                        if 'generated_vol_text' in st.session_state:
                            del st.session_state['generated_vol_text']
                        if 'vol_text_area_final' in st.session_state:
                            del st.session_state['vol_text_area_final']
                        st.rerun()
                    else:
                        st.error("Введите текст!")
            else:
                st.warning("Редактор еще не создал шаблоны.")

    st.markdown("---")
    # Кнопка публикации только для СММ и Руководителя
    if role in ["СММ", "Руководитель", "Администратор"]:
        if st.button("🚀 Опубликовать следующий по приоритету"):
            publish_next_post()
            st.rerun()

if role != "Наблюдатель":
 with tab_q:
    if role in ["СММ", "Руководитель", "Администратор"]:
        st.header("📅 План публикаций")
        st.markdown(f"## {datetime.now().strftime('%B %Y').capitalize()}")
    else:
        st.header("📋 Очередь постов")
    
    conn = get_connection()
    queue_data = pd.read_sql("SELECT id, suggested_text, scheduled_at, status, priority FROM post_queue", conn)
    vk_posted_data = pd.read_sql("SELECT id, date, text FROM posts ORDER BY date DESC LIMIT 300", conn)
    conn.close()
    if not queue_data.empty:
        queue_data["scheduled_at"] = pd.to_datetime(queue_data["scheduled_at"], errors="coerce")

    # 1. Кастомный календарь с popover над клеткой для редактора, СММ и руководителя
    if role in ["Редактор", "СММ", "Руководитель", "Администратор"]:
        st.subheader("Визуальный календарь (Popup)")
        posts_payload = []
        if not queue_data.empty:
            q = queue_data.where(pd.notnull(queue_data), None)
            for _, row in q.iterrows():
                sched = row["scheduled_at"]
                sched_iso = sched.isoformat() if sched is not None and pd.notna(sched) else None
                posts_payload.append(
                    {
                        "id": int(row["id"]),
                        "suggested_text": row["suggested_text"] or "",
                        "scheduled_at": sched_iso,
                        "status": row["status"] or "pending",
                    }
                )
        if not vk_posted_data.empty:
            p = vk_posted_data.where(pd.notnull(vk_posted_data), None)
            for _, row in p.iterrows():
                dt = row["date"]
                dt_iso = pd.to_datetime(dt, errors="coerce")
                dt_iso = dt_iso.isoformat() if dt is not None and pd.notna(dt_iso) else None
                posts_payload.append(
                    {
                        # Отрицательный ID = архивный пост из VK (read-only в попапе)
                        "id": -int(row["id"]),
                        "suggested_text": row["text"] or "",
                        "scheduled_at": dt_iso,
                        "status": "posted",
                    }
                )
        action = queue_calendar_popup_component(posts_payload, key="queue_calendar_popup_main")
        if action and isinstance(action, dict):
            if action.get("action") == "save":
                if int(action.get("post_id")) < 0:
                    st.info("Архивный пост из VK доступен только для просмотра.")
                    st.rerun()
                conn = get_connection(); cur = conn.cursor()
                cur.execute(
                    "UPDATE post_queue SET suggested_text = %s, status = %s WHERE id = %s",
                    (action.get("text", ""), action.get("status", "pending"), int(action.get("post_id"))),
                )
                conn.commit(); cur.close(); conn.close()
                st.success(f"Пост #{action.get('post_id')} обновлен")
                st.rerun()
            elif action.get("action") == "delete":
                if int(action.get("post_id")) < 0:
                    try:
                        import vk_api
                        group_id = os.getenv("GROUP_ID", "").strip().replace("-", "")
                        if not group_id:
                            st.error("Не задан GROUP_ID в .env")
                            st.rerun()
                        vk = vk_api.VkApi(token=os.getenv("VK_USER_TOKEN")).get_api()
                        vk_post_id = abs(int(action.get("post_id")))
                        vk.wall.delete(owner_id=-int(group_id), post_id=vk_post_id)
                        conn = get_connection(); cur = conn.cursor()
                        cur.execute("DELETE FROM posts WHERE id = %s", (vk_post_id,))
                        conn.commit(); cur.close(); conn.close()
                        st.success(f"Пост VK #{vk_post_id} удален")
                    except Exception as e:
                        st.error(f"Не удалось удалить пост в VK: {e}")
                    st.rerun()
                conn = get_connection(); cur = conn.cursor()
                cur.execute("DELETE FROM post_queue WHERE id = %s", (int(action.get("post_id")),))
                conn.commit(); cur.close(); conn.close()
                st.warning(f"Пост #{action.get('post_id')} удален")
                st.rerun()
            elif action.get("action") == "select":
                st.caption(f"Выбрано: {action.get('date')} · {action.get('status')}")
        st.divider()

    # 2. Таблица для всех
    st.subheader("📋 Список очереди")
    if not queue_data.empty:
        st.dataframe(queue_data, width='stretch')
    else:
        st.write("Очередь пуста.")

with tab_a:
    st.header("Архив постов из VK")
    conn = get_connection()
    archive_df = pd.read_sql(
        """
        SELECT
            q.created_at AS date,
            q.title AS `Заголовок публикации`,
            q.author_code AS `Автор`,
            q.approver_code AS `Кто выпустил`,
            q.suggested_text AS text,
            NULL AS likes,
            NULL AS views,
            NULL AS er
        FROM post_queue q
        WHERE q.status = 'posted'
        UNION ALL
        SELECT
            p.date AS date,
            LEFT(COALESCE(p.text, 'Без заголовка'), 120) AS `Заголовок публикации`,
            NULL AS `Автор`,
            NULL AS `Кто выпустил`,
            p.text AS text,
            p.likes AS likes,
            p.views AS views,
            (p.likes+p.reposts+p.comments)/NULLIF(p.views,0)*100 AS er
        FROM posts p
        ORDER BY date DESC
        LIMIT 80
        """,
        conn,
    )
    conn.close()
    st.dataframe(archive_df, width='stretch')

st.markdown("---")
st.caption("HacatonProTechno - Система автоматизации работы волонтеров")
