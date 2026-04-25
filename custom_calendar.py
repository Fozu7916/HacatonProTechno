import os
import streamlit.components.v1 as components


_COMPONENT = components.declare_component(
    "queue_calendar_popup",
    path=os.path.join(os.path.dirname(__file__), "custom_calendar_component"),
)


def queue_calendar_popup_component(posts, key="queue_calendar_popup"):
    """
    Кастомный календарь с попапом над клеткой/плашкой.
    Возвращает dict с действием:
      - {"action": "save", "post_id": int, "text": str, "status": str}
      - {"action": "delete", "post_id": int}
      - {"action": "select", "date": "YYYY-MM-DD", "status": "..."}
    """
    return _COMPONENT(posts=posts, key=key, default=None)
