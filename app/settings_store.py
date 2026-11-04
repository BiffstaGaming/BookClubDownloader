"""Key/value settings stored in the database, with defaults for newer keys."""
from sqlalchemy.orm import Session

from app.models import Setting

SETTING_KEYS = [
    "abook_url",
    "abook_username",
    "abook_password",
    "forum_cache_hours",
    "nzbget_url",
    "nzbget_username",
    "nzbget_password",
    "nzbget_category",
    "nzbget_path_prefix",
    "local_path_prefix",
    "m4b_output_path",
    "m4b_move_template",
    "m4b_jobs",
    "m4b_bitrate",
    "m4b_channels",
    "m4b_samplerate",
    "encode_max_cores",
    "delete_source",
    "abs_url",
    "abs_token",
    "abs_library_id",
    "abs_path_local",
    "abs_path_remote",
    "auto_convert",
    "auto_match_threshold",
    "audible_provider",
    "preferred_language",
    "ha_webhook_url",
    "ha_events",
    "public_url",
    "api_token",
    "ai_enabled",
    "claude_oauth_token",
    "ai_model",
    "ai_daily_limit",
    "ai_pause_above_percent",
    "ai_pick_requests",
    "poll_interval",
    "log_retention_days",
]

# Secrets are never rendered back into the settings page; a blank field keeps the saved value.
SECRET_KEYS = {"abook_password", "nzbget_password", "abs_token", "claude_oauth_token"}

# Checkbox settings: an unticked box submits nothing, which means "0".
BOOL_KEYS = {"delete_source", "auto_convert", "ai_enabled", "ai_pick_requests"}

# Lists of checkboxes, stored comma-separated. Nothing ticked is stored as "none".
MULTI_KEYS = {"ha_events"}

DEFAULTS = {
    "abook_url": "https://abook.link/book/index.php",
    "m4b_output_path": "/m4b",
    "delete_source": "1",
    "auto_convert": "1",
    "auto_match_threshold": "90",
    "audible_provider": "audible",
    "preferred_language": "English",
    "poll_interval": "15",
    "log_retention_days": "14",
    "forum_cache_hours": "12",
    # Speech-friendly AAC: mono at 22.05 kHz is about a quarter of the encoding work of
    # 44.1 kHz stereo, and sounds the same for a single narrator.
    "m4b_bitrate": "64k",
    "m4b_channels": "1",
    "m4b_samplerate": "22050",
    "ai_model": "sonnet",
    "ai_daily_limit": "40",
    "ai_pause_above_percent": "60",
    "ai_pick_requests": "1",
}

# Claude Code model aliases, so the newest model of each kind is used.
AI_MODELS = [
    ("haiku", "Haiku — fastest, lightest on your plan"),
    ("sonnet", "Sonnet — recommended"),
    ("opus", "Opus — most careful, uses the most of your plan"),
]

# Audible editions in other languages often share the English title exactly;
# a non-matching edition is scored down so it is never auto-picked. "any" = no preference.
LANGUAGES = ["English", "German", "French", "Spanish", "Italian", "Dutch", "Portuguese",
             "Swedish", "Danish", "Norwegian", "Finnish", "Polish", "Japanese"]

AUDIBLE_PROVIDERS = [
    ("audible", "Audible.com (US)"),
    ("audible.uk", "Audible.co.uk"),
    ("audible.ca", "Audible.ca"),
    ("audible.au", "Audible.com.au"),
    ("audible.de", "Audible.de"),
    ("audible.fr", "Audible.fr"),
    ("audible.it", "Audible.it"),
    ("audible.es", "Audible.es"),
    ("audible.in", "Audible.in"),
    ("audible.jp", "Audible.co.jp"),
]


def get_setting(db: Session, key: str, default: str = None) -> str:
    row = db.query(Setting).filter(Setting.key == key).first()
    if row and row.value not in (None, ""):
        return row.value
    if default is not None:
        return default
    return DEFAULTS.get(key, "")


def set_setting(db: Session, key: str, value: str):
    row = db.query(Setting).filter(Setting.key == key).first()
    if row:
        row.value = value
    else:
        db.add(Setting(key=key, value=value))
    db.commit()


def get_bool(db: Session, key: str) -> bool:
    return get_setting(db, key) == "1"


def get_int(db: Session, key: str, fallback: int) -> int:
    try:
        return int(get_setting(db, key))
    except (TypeError, ValueError):
        return fallback


def all_settings(db: Session) -> dict:
    return {key: get_setting(db, key) for key in SETTING_KEYS}
