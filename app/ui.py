"""Small HTML fragments returned directly from routes (always escaped)."""
from fastapi.responses import HTMLResponse
from markupsafe import escape

_ICONS = {
    "success": "check-circle-fill",
    "info": "info-circle-fill",
    "warning": "exclamation-triangle-fill",
    "danger": "exclamation-triangle-fill",
}

_CLOSE = '<button type="button" class="btn-close btn-close-sm ms-2" data-bs-dismiss="alert" aria-label="Close"></button>'


def alert_html(kind: str, message: str, extra_html: str = "", classes: str = "") -> str:
    """
    Bootstrap alert with an escaped message. `extra_html` must already be safe.
    Every alert can be closed; success and info alerts also fade away by
    themselves (see base.html).
    """
    icon = _ICONS.get(kind, "info-circle-fill")
    autohide = ' data-autohide="1"' if kind in ("success", "info") else ""
    return (
        f'<div class="alert alert-{kind} alert-dismissible d-flex align-items-start {classes}" role="alert"{autohide}>'
        f'<i class="bi bi-{icon} me-2 mt-1"></i><div class="flex-grow-1">{escape(message)}{extra_html}</div>{_CLOSE}</div>'
    )


def alert(kind: str, message: str, extra_html: str = "", classes: str = "") -> HTMLResponse:
    return HTMLResponse(alert_html(kind, message, extra_html, classes))


def not_configured(service: str) -> HTMLResponse:
    return alert(
        "warning",
        f"{service} is not configured. ",
        '<a href="/settings" class="alert-link">Go to Settings</a>',
    )
