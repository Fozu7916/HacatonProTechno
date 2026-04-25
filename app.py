import streamlit as st
import pandas as pd
import os
import time
import requests
import io
from datetime import datetime
from streamlit_calendar import calendar
from custom_calendar import queue_calendar_popup_component
from database.db import get_connection, init_db, get_setting, set_setting, add_template, get_templates, add_to_queue
from analytics.stats import get_dry_stats
from analytics.processor import process_incoming_post
from publisher.scheduler import publish_next_post
st.set_page_config(page_title="VK Volunteer Panel", layout="wide")

st.title("📱 Панель управления контентом")


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
                    "Пиши живо, структурно, без канцелярита, на русском языке."
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
role = st.sidebar.selectbox("Выберите роль", ["Волонтер", "Редактор", "СММ-специалист", "Руководитель"])

st.sidebar.header("⚙️ Управление")
# Слайдер теперь от 1 поста
stat_limit = st.sidebar.slider("Глубина анализа (постов)", 1, 200, 50)

if st.sidebar.button("🔄 Обновить данные из VK"):
    with st.sidebar.status("Парсинг..."):
        from parser.vk_parser import parse_all_posts
        from database.db import upsert_post
        data = parse_all_posts(n=20)
        for item in data:
            upsert_post(item["post"])
    st.sidebar.success("Обновлено!")
    st.rerun()

# Настройки и отчеты только для Руководителя
if role == "Руководитель":
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
                st.line_chart(report_data["chart_data"], use_container_width=True)
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
                st.bar_chart(summary_df.set_index("Показатель"), use_container_width=True)

            st.markdown("### 📅 Ежедневная статистика")
            st.dataframe(report_data["daily_stats"], use_container_width=True)

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
                        use_container_width=True,
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
                        use_container_width=True,
                    )
            except Exception as e:
                st.error(f"Ошибка генерации Excel: {e}")
st.sidebar.header("📊 Статистика")
stats, _ = get_dry_stats(limit=stat_limit)
if stats:
    for key, val in stats.items():
        st.sidebar.metric(key, val)

# --- ОСНОВНАЯ ОБЛАСТЬ ---
if role == "Руководитель":
    tab_list = ["📅 Очередь", "📈 Архив"]
    tabs = st.tabs(tab_list)
    tab_q, tab_a = tabs[0], tabs[1]
else:
    tab_list = ["📝 Редактор", "📅 Очередь", "📈 Архив"]
    tabs = st.tabs(tab_list)
    tab_r, tab_q, tab_a = tabs[0], tabs[1], tabs[2]

# Логика вкладки Редактор (только если она есть)
if role != "Руководитель":
    with tab_r:
        if role == "СММ-специалист":
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
                smm_text = st.text_area("Текст официального поста", height=150, key="smm_text")
                smm_uploaded_file = st.file_uploader("Прикрепите фото", type=['png', 'jpg', 'jpeg'], key="smm_photo_free")
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
                            ai_text = generate_text_with_ai(ai_prompt, role_hint="СММ-специалист")
                            st.session_state['generated_smm_text'] = ai_text
                            st.session_state['smm_text_afisha_area'] = ai_text
                            st.success("ИИ-текст сгенерирован")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Ошибка ИИ-генерации: {e}")
                    
                    if "smm_text_afisha_area" not in st.session_state:
                        st.session_state["smm_text_afisha_area"] = st.session_state.get("generated_smm_text", "")
                    smm_text = st.text_area(
                        "Текст афиши (на основе шаблона)",
                        height=150,
                        key="smm_text_afisha_area"
                    )
                    smm_uploaded_file = st.file_uploader("Прикрепите фото афиши", type=['png', 'jpg', 'jpeg'], key="smm_photo_afisha")
                else:
                    st.warning("Нет доступных шаблонов.")
                    smm_text = ""
                    smm_uploaded_file = None

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
                        smm_photo = None
                        if smm_uploaded_file:
                            try:
                                from parser.vk_parser import upload_photo
                                import vk_api
                                vk = vk_api.VkApi(token=os.getenv("VK_USER_TOKEN")).get_api()
                                smm_photo = upload_photo(vk, smm_uploaded_file)
                            except Exception as e:
                                st.error(f"Ошибка загрузки фото: {e}")
                        
                        from database.db import add_to_queue
                        add_to_queue(None, smm_text, priority=5, er=0.0, scheduled_at=scheduled_at, attachments=smm_photo)
                        
                        # СММ посты сразу получают статус 'ready', им не нужно одобрение редактора
                        conn = get_connection(); cur = conn.cursor()
                        cur.execute("UPDATE post_queue SET status = 'ready', author_role = 'smm' WHERE suggested_text = %s ORDER BY created_at DESC LIMIT 1", (smm_text,))
                        conn.commit(); cur.close(); conn.close()
                        
                        st.success(f"Запланировано на {scheduled_at.strftime('%d.%m %H:%M')}")
                    else:
                        st.error("Текст пуст!")

        elif role == "Редактор":
            st.header("🛠 Управление контентом (Редактор)")
            
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
                post_to_fix = st.selectbox("Выберите пост для проверки", to_edit['id'])
                current_row = to_edit[to_edit['id'] == post_to_fix].iloc[0]
                
                st.info(f"Текст волонтера:\n{current_row['suggested_text']}")
                if current_row['attachments']:
                    st.write(f"Вложение: {current_row['attachments']}")
                    # Если это локальный путь, показываем напрямую
                    if os.path.exists(str(current_row['attachments'])):
                        st.image(current_row['attachments'], caption="Предпросмотр (локально)", width=400)
                    else:
                        try:
                            from parser.vk_parser import get_photo_url
                            import vk_api
                            vk = vk_api.VkApi(token=os.getenv("VK_USER_TOKEN")).get_api()
                            img_url = get_photo_url(vk, current_row['attachments'])
                            if img_url:
                                st.image(img_url, caption="Прикрепленное фото", width=400)
                            else:
                                vk_link = f"https://vk.com/{current_row['attachments']}"
                                st.warning("Не удалось загрузить предпросмотр. Посмотрите фото по ссылке:")
                                st.link_button("🔗 Открыть фото в VK", vk_link)
                        except Exception as e:
                            st.warning(f"Не удалось загрузить предпросмотр фото: {e}")
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Одобрить (в очередь)"):
                        conn = get_connection(); cur = conn.cursor()
                        cur.execute("UPDATE post_queue SET status = 'ready', priority = 3 WHERE id = %s", (post_to_fix,))
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
                final_text = st.text_area(
                    "Итоговый текст (можно подправить)",
                    height=200,
                    key="vol_text_area_final"
                )
                uploaded_file = st.file_uploader("Прикрепите фото", type=['png', 'jpg', 'jpeg'])
                
                if st.button("Отправить на проверку"):
                    if final_text:
                        attachments = None
                        if uploaded_file:
                            # Сохраняем локально для предпросмотра
                            import os
                            if not os.path.exists("uploads"): os.makedirs("uploads")
                            file_path = os.path.join("uploads", f"{int(time.time())}_{uploaded_file.name}")
                            with open(file_path, "wb") as f:
                                f.write(uploaded_file.getbuffer())
                            attachments = file_path # Сохраняем путь к локальному файлу
                        
                        from database.db import add_to_queue
                        add_to_queue(None, final_text, priority=2, er=0.0, attachments=attachments)
                        
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
    if role in ["СММ-специалист", "Руководитель"]:
        if st.button("🚀 Опубликовать следующий по приоритету"):
            publish_next_post()
            st.rerun()

with tab_q:
    if role in ["СММ-специалист", "Руководитель"]:
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
    if role in ["Редактор", "СММ-специалист", "Руководитель"]:
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
    archive_df = pd.read_sql("SELECT date, text, likes, views, er FROM (SELECT *, (likes+reposts+comments)/NULLIF(views,0)*100 as er FROM posts) t ORDER BY date DESC LIMIT 50", conn)
    conn.close()
    st.dataframe(archive_df, width='stretch')

st.markdown("---")
st.caption("HacatonProTechno - Система автоматизации работы волонтеров")
