"""Key/value settings stored in the database, with defaults for newer keys."""
from sqlalchemy.orm import Session

from app.models import Setting

SETTING_KEYS = [
    "abook_url",
    "abook_username",
    "abook_password",
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
    "poll_interval",
    "log_retention_days",
]

# Secrets are never rendered back into the settings page; a blank field keeps the saved value.
SECRET_KEYS = {"abook_password", "nzbget_password", "abs_token"}

# Checkbox settings: an unticked box submits nothing, which means "0".
BOOL_KEYS = {"delete_source", "auto_convert"}

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
}

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
