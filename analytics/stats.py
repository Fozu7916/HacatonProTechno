import mysql.connector
import os
from dotenv import load_dotenv
import pandas as pd
import argparse
import warnings
from datetime import datetime

# Скрываем предупреждение pandas о прямом подключении через DBAPI2
warnings.filterwarnings("ignore", category=UserWarning)


load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
    )

def get_dry_stats(limit=100):
    """Получает сухую статистику и топ постов."""
    try:
        conn = get_connection()
        query = f"""
            SELECT 
                id, owner_id, likes, reposts, views, comments, date, text 
            FROM posts 
            ORDER BY date DESC 
            LIMIT {limit}
        """
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"[!] Ошибка подключения к БД: {e}")
        return None, None

    if df.empty:
        return None, None


    stats = {
        "Всего постов": len(df),
        "Средние лайки": round(df['likes'].mean(), 2),
        "Средние репосты": round(df['reposts'].mean(), 2),
        "Средние просмотры": round(df['views'].mean(), 2),
        "Средние комменты": round(df['comments'].mean(), 2),
    }
    
    # ER (Engagement Rate)
    total_interactions = df['likes'] + df['reposts'] + df['comments']
    df['er'] = (total_interactions / df['views'].replace(0, 1)) * 100
    stats["Средний ER (%)"] = round(df['er'].mean(), 2)


    top_posts = df.sort_values(by='views', ascending=False).head(3)
    
    return stats, top_posts

def get_stats_by_days(days=7):
    """Получает статистику за последние N дней."""
    conn = get_connection()
    query = f"""
        SELECT 
            id, owner_id, likes, reposts, views, comments, date, text 
        FROM posts 
        WHERE date >= DATE_SUB(NOW(), INTERVAL {days} DAY)
        ORDER BY date DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    if df.empty:
        return None
    
    total_interactions = df['likes'] + df['reposts'] + df['comments']
    df['er'] = (total_interactions / df['views'].replace(0, 1)) * 100
    
    # --- Расширенный анализ ---
    # 1. Лучшее время (Топ-3 часа)
    df['hour'] = df['date'].dt.hour
    best_hours = df.groupby('hour')['er'].mean().sort_values(ascending=False).head(3).index.tolist()
    
    # 2. Ключевые слова (простой счетчик слов > 4 символов)
    import re
    from collections import Counter
    words = []
    for text in df['text'].dropna():
        clean_text = re.sub(r'[^\w\s]', '', text.lower())
        words.extend([w for w in clean_text.split() if len(w) > 4])
    common_words = [word for word, count in Counter(words).most_common(5)]

    # 3. Топ-3 поста по просмотрам
    top_3 = df.sort_values(by='views', ascending=False).head(3)
    top_3_list = []
    for _, row in top_3.iterrows():
        top_3_list.append({
            "views": int(row['views']),
            "likes": int(row['likes']),
            "link": f"https://vk.com/wall{row['owner_id']}_{row['id']}",
            "text": row['text'][:50] + "..." if row['text'] else "Без текста"
        })

    # 4. Анализ по дням недели
    df['day_name'] = df['date'].dt.day_name()
    best_day = df.groupby('day_name')['er'].mean().idxmax()

    return {
        "stats": {
            "Период (дней)": days,
            "Всего постов": len(df),
            "Всего охвата": int(df['views'].sum()),
            "Средний ER (%)": round(df['er'].mean(), 2),
            "Лучшие часы": ", ".join([f"{h}:00" for h in best_hours]),
            "Лучший день недели": best_day,
            "Ключевые темы": ", ".join(common_words)
        },
        "chart_data": df[['date', 'views', 'likes']].set_index('date'),
        "daily_stats": df.groupby(df['date'].dt.date).agg({
            'views': 'sum',
            'likes': 'sum',
            'reposts': 'sum',
            'comments': 'sum'
        }).sort_index(ascending=False),
        "top_3": top_3_list
    }

def generate_pdf_report(report_data, chart_df=None):
    """Генерирует подробный PDF отчет."""
    from fpdf import FPDF
    import matplotlib.pyplot as plt
    import io
    
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    epw = pdf.w - pdf.l_margin - pdf.r_margin
    
    # Шрифты
    possible_fonts = ["C:/Windows/Fonts/times.ttf", "C:/Windows/Fonts/arial.ttf"]
    font_loaded = False
    for font_path in possible_fonts:
        if os.path.exists(font_path):
            try:
                pdf.add_font('CustomFont', '', font_path); pdf.set_font('CustomFont', '', 16)
                font_loaded = True; break
            except: continue
    if not font_loaded: pdf.set_font('helvetica', '', 16)

    pdf.cell(epw, 10, txt="Community Efficiency Report (VK)", ln=True, align='C')
    pdf.ln(5)
    
    if font_loaded: pdf.set_font('CustomFont', '', 11)
    else: pdf.set_font('helvetica', '', 11)

    # 1) KPI + инсайты (как в дашборде)
    for key, value in report_data['stats'].items():
        pdf.set_x(pdf.l_margin)
        pdf.multi_cell(epw, 7, txt=f"{key}: {value}")
    
    # 2) График динамики (как в дашборде)
    if chart_df is not None and not chart_df.empty:
        pdf.ln(5)
        plt.figure(figsize=(10, 4))
        plt.plot(chart_df.index, chart_df['views'], color='blue', label='Views')
        plt.plot(chart_df.index, chart_df['likes'], color='red', label='Likes')
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        
        img_buf = io.BytesIO()
        plt.savefig(img_buf, format='png', dpi=150)
        img_buf.seek(0)
        img_w = min(epw, 190)
        pdf.image(img_buf, x=pdf.l_margin, y=pdf.get_y(), w=img_w)
        plt.close()
        pdf.ln(75) # Делаем большой отступ после графика, чтобы текст не накладывался

    # 3) Суммарные метрики (bar chart, как в дашборде)
    daily_stats = report_data.get('daily_stats')
    if daily_stats is not None and not daily_stats.empty:
        summary_values = {
            "Views": int(daily_stats['views'].sum()),
            "Likes": int(daily_stats['likes'].sum()),
            "Reposts": int(daily_stats['reposts'].sum()),
            "Comments": int(daily_stats['comments'].sum()),
        }
        plt.figure(figsize=(8, 3.5))
        plt.bar(list(summary_values.keys()), list(summary_values.values()), color=["#2563eb", "#ef4444", "#22c55e", "#f59e0b"])
        plt.grid(axis='y', alpha=0.25)
        plt.tight_layout()
        img_buf2 = io.BytesIO()
        plt.savefig(img_buf2, format='png', dpi=150)
        img_buf2.seek(0)
        if pdf.get_y() > 190:
            pdf.add_page()
        pdf.image(img_buf2, x=pdf.l_margin, y=pdf.get_y(), w=min(epw, 170))
        plt.close()
        pdf.ln(65)

    # 4) Таблица подробной статистики
    pdf.set_font('CustomFont', '', 12) if font_loaded else pdf.set_font('helvetica', '', 12)
    pdf.cell(epw, 10, txt="Подробная статистика по дням:", ln=True)
    pdf.set_font('CustomFont', '', 9) if font_loaded else pdf.set_font('helvetica', '', 9)
    
    # Заголовки таблицы
    c1, c2, c3, c4 = 50, 45, 40, 45
    if c1 + c2 + c3 + c4 > epw:
        scale = epw / (c1 + c2 + c3 + c4)
        c1, c2, c3, c4 = c1 * scale, c2 * scale, c3 * scale, c4 * scale
    pdf.cell(c1, 8, "Дата", 1)
    pdf.cell(c2, 8, "Просмотры", 1)
    pdf.cell(c3, 8, "Лайки", 1)
    pdf.cell(c4, 8, "Комменты", 1)
    pdf.ln()
    
    # Данные таблицы (последние 10 дней для компактности)
    daily = report_data['daily_stats'].head(10)
    for date, row in daily.iterrows():
        pdf.cell(c1, 7, str(date), 1)
        pdf.cell(c2, 7, str(int(row['views'])), 1)
        pdf.cell(c3, 7, str(int(row['likes'])), 1)
        pdf.cell(c4, 7, str(int(row['comments'])), 1)
        pdf.ln()

    # 5) Топ-3 поста (как в дашборде)
    top_3 = report_data.get('top_3', [])
    if top_3:
        if pdf.get_y() > 215:
            pdf.add_page()
        pdf.ln(4)
        pdf.set_font('CustomFont', '', 12) if font_loaded else pdf.set_font('helvetica', '', 12)
        pdf.cell(epw, 10, txt="Топ-3 лучших поста:", ln=True)
        pdf.set_font('CustomFont', '', 10) if font_loaded else pdf.set_font('helvetica', '', 10)
        for i, post in enumerate(top_3, start=1):
            line = f"{i}) Views: {post.get('views', 0)} | Likes: {post.get('likes', 0)}"
            pdf.set_x(pdf.l_margin)
            pdf.multi_cell(0, 7, txt=line)
            txt = str(post.get('text', 'Без текста'))
            if len(txt) > 180:
                txt = txt[:180] + "..."
            pdf.set_x(pdf.l_margin)
            pdf.multi_cell(0, 6, txt=f"Текст: {txt}")
            link = str(post.get('link', ''))
            if link:
                pdf.set_x(pdf.l_margin)
                pdf.multi_cell(0, 6, txt=f"Ссылка: {link}")
            pdf.ln(2)

    pdf.ln(10)
    pdf.set_font('helvetica', '', 8)
    pdf.cell(epw, 10, txt=f"Generated: {datetime.now().strftime('%d.%m.%Y %H:%M')}", ln=True, align='R')
    
    return pdf.output()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Аналитика постов VK")
    parser.add_argument("-c", "--count", type=int, default=100, help="Количество постов для анализа")
    args = parser.parse_args()

    print(f"[*] Анализ последних {args.count} постов...")
    stats, top = get_dry_stats(limit=args.count)
    
    if stats:
        print("\n" + "="*40)
        print(f"{'ОБЩАЯ СТАТИСТИКА':^40}")
        print("="*40)
        for key, value in stats.items():
            print(f"{key:25}: {value}")
        
        print("\n" + "="*40)
        print(f"{'ТОП-3 ЛУЧШИХ ПОСТА':^40}")
        print("="*40)
        for i, (idx, row) in enumerate(top.iterrows()):
            link = f"https://vk.com/wall{row['owner_id']}_{row['id']}"
            print(f"{i+1}. Просмотры: {int(row['views'])} | Лайки: {int(row['likes'])}")
            print(f"   Ссылка: {link}")
            text_preview = row['text'][:80].replace('\n', ' ') + "..." if row['text'] else "Без текста"
            print(f"   Текст: {text_preview}")
            print("-" * 40)
    else:
        print("\n[!] Данные не найдены. Сначала запустите парсер (python parser/main.py).")
