"""The Claude reviewer: CLI output parsing, limits, and only trusting known answers."""
import json
import time

import pytest

from app.services import ai


def stream(*events) -> str:
    return "\n".join(json.dumps(e) for e in events)


def result(text: str, **extra) -> dict:
    return {"type": "result", "result": text, "usage": {"input_tokens": 100, "output_tokens": 20}, **extra}


@pytest.fixture
def enabled(settings, monkeypatch):
    settings(ai_enabled="1", claude_oauth_token="tok", ai_daily_limit="3")
    monkeypatch.setattr(ai, "cli_path", lambda: "/usr/local/bin/claude")
    calls = []

    def fake(prompt, model, token):
        calls.append((prompt, model, token))
        return fake.reply
    fake.reply = ai.ClaudeResult(text='{"choice": 2, "confidence": 95, "reason": "length matches"}')
    monkeypatch.setattr(ai, "_call", fake)
    return fake, calls


def test_parse_stream_reads_reply_usage_and_plan():
    out = stream(
        {"type": "system", "subtype": "init", "tools": [], "mcp_servers": []},
        {"type": "rate_limit_event", "rate_limit_info": {"unifiedWindows": {"five_hour": {"utilization": 0.42}}}},
        result('{"ok": true}'),
    )
    parsed = ai.parse_stream(out)
    assert parsed.text == '{"ok": true}' and parsed.input_tokens == 100
    assert parsed.windows["five_hour"]["utilization"] == 0.42


def test_reply_is_refused_if_claude_had_tools():
    with pytest.raises(ai.ClaudeError, match="tools"):
        ai.parse_stream(stream({"type": "system", "subtype": "init", "tools": ["Bash"]}, result("{}")))


def test_error_result_raises():
    with pytest.raises(ai.ClaudeError, match="Not logged in"):
        ai.parse_stream(stream(result("Not logged in", is_error=True)))


def test_unavailable_reasons(db, settings, monkeypatch):
    assert "turned off" in ai.unavailable_reason(db)
    settings(ai_enabled="1")
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    assert "setup-token" in ai.unavailable_reason(db)
    settings(claude_oauth_token="tok")
    monkeypatch.setattr(ai, "cli_path", lambda: "")
    assert "not installed" in ai.unavailable_reason(db)


def test_review_match_only_accepts_a_candidate_we_sent(db, enabled):
    fake, calls = enabled
    candidates = [{"title": "Dune", "duration": 300}, {"title": "Dune", "duration": 1260}]
    verdict = ai.review_match(db, {"forum topic": "Dune"}, candidates, "English")
    assert verdict["index"] == 1 and verdict["confidence"] == 95
    assert "length: 21h 00m" in calls[0][0] and calls[0][1] == "sonnet" and calls[0][2] == "tok"

    fake.reply = ai.ClaudeResult(text='{"choice": 7, "confidence": 99}')
    assert ai.review_match(db, {}, candidates, "English")["index"] is None
    fake.reply = ai.ClaudeResult(text="I think it's the second one")
    assert ai.review_match(db, {}, candidates, "English")["index"] is None


def test_daily_limit(db, enabled):
    for _ in range(3):
        ai.ask(db, "x")
    with pytest.raises(ai.ClaudeError, match="limit"):
        ai.ask(db, "x")
    assert ai.usage(db)["calls"] == 3


def test_failed_calls_do_not_count(db, enabled, monkeypatch):
    def boom(*a):
        raise ai.ClaudeError("network")
    monkeypatch.setattr(ai, "_call", boom)
    with pytest.raises(ai.ClaudeError):
        ai.ask(db, "x")
    assert ai.usage(db)["calls"] == 0 and ai.usage(db)["failures"] == 1


def test_pauses_when_plan_is_busy(db, enabled, settings):
    fake, _ = enabled
    future = time.time() + 3600
    fake.reply = ai.ClaudeResult(text="{}", windows={"five_hour": {"utilization": 0.8, "resetsAt": future}})
    ai.ask(db, "x")
    assert "5-hour" in ai.unavailable_reason(db)
    settings(ai_pause_above_percent="90")
    assert ai.unavailable_reason(db) == ""
