"""Small HTML fragments returned directly from routes (always escaped)."""
from fastapi.responses import HTMLResponse
from markupsafe import escape

_ICONS = {
    "success": "check-circle-fill",
    "info": "info-circle-fill",
    "warning": "exclamation-triangle-fill",
    "danger": "exclamation-triangle-fill",
}


def alert_html(kind: str, message: str, extra_html: str = "", classes: str = "") -> str:
    """Bootstrap alert with an escaped message. `extra_html` must already be safe."""
    icon = _ICONS.get(kind, "info-circle-fill")
    return (
        f'<div class="alert alert-{kind} {classes}">'
        f'<i class="bi bi-{icon} me-2"></i>{escape(message)}{extra_html}</div>'
    )


def alert(kind: str, message: str, extra_html: str = "", classes: str = "") -> HTMLResponse:
    return HTMLResponse(alert_html(kind, message, extra_html, classes))


def not_configured(service: str) -> HTMLResponse:
    return alert(
        "warning",
        f"{service} is not configured. ",
        '<a href="/settings" class="alert-link">Go to Settings</a>',
    )
