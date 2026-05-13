from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def current_date_yyyymmdd() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")