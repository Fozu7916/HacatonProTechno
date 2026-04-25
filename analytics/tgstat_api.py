import requests


def _num(val, default=0.0):
    try:
        if val is None:
            return float(default)
        return float(val)
    except Exception:
        return float(default)


def _pick(d: dict, keys: list[str], default=0.0):
    for k in keys:
        if k in d and d[k] is not None:
            return _num(d[k], default)
    return _num(default, default)


def get_tgstat_summary(api_token: str, channels: list[str]) -> dict:
    """
    Возвращает агрегированную статистику TGStat по каналам:
    subscribers, avg_reach, avg_views, err.
    """
    token = (api_token or "").strip()
    items = [str(c).strip() for c in (channels or []) if str(c).strip()]
    if not token or not items:
        return {
            "channels_found": 0,
            "subscribers": 0,
            "avg_reach": 0,
            "avg_views": 0,
            "avg_err": 0,
            "reach_total": 0,
            "coverage_rate": 0,
        }

    rows = []
    for channel in items:
        try:
            resp = requests.get(
                "https://api.tgstat.ru/channels/stat",
                params={"token": token, "channelId": channel},
                timeout=20,
            )
            if not resp.ok:
                continue
            data = resp.json() or {}
            payload = data.get("response") or data.get("data") or {}
            if isinstance(payload, list):
                payload = payload[0] if payload else {}
            if not isinstance(payload, dict):
                continue

            subscribers = _pick(payload, ["participants_count", "subscribers", "members", "followers"], 0)
            avg_reach = _pick(payload, ["avg_reach", "reach", "coverage", "avg_post_reach"], 0)
            avg_views = _pick(payload, ["avg_views", "views", "avg_post_views"], avg_reach)
            err = _pick(payload, ["err", "er", "engagement_rate"], 0)

            rows.append(
                {
                    "channel": channel,
                    "subscribers": subscribers,
                    "avg_reach": avg_reach,
                    "avg_views": avg_views,
                    "err": err,
                }
            )
        except Exception:
            continue

    if not rows:
        return {
            "channels_found": 0,
            "subscribers": 0,
            "avg_reach": 0,
            "avg_views": 0,
            "avg_err": 0,
            "reach_total": 0,
            "coverage_rate": 0,
        }

    n = len(rows)
    subscribers_total = sum(r["subscribers"] for r in rows)
    reach_total = sum(r["avg_reach"] for r in rows)
    coverage_rate = (reach_total / subscribers_total * 100) if subscribers_total else 0
    return {
        "channels_found": n,
        "subscribers": int(subscribers_total),
        "avg_reach": round(sum(r["avg_reach"] for r in rows) / n, 2),
        "avg_views": round(sum(r["avg_views"] for r in rows) / n, 2),
        "avg_err": round(sum(r["err"] for r in rows) / n, 2),
        "reach_total": int(reach_total),
        "coverage_rate": round(coverage_rate, 2),
    }
