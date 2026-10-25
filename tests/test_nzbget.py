import json

from app.services import nzbget


def nzbget_read_pp_parameters(params_text: str) -> dict:
    """
    Python port of how NZBGet (daemon/remote/XmlRpc.cpp + WebUtil::JsonNextValue)
    reads the PPParameters argument of `append` over JSON-RPC: it skips
    ` ,[{:` and whitespace, takes the next string token — object keys included —
    pairs the tokens up as name, value, and does NOT unescape them.
    """
    tokens, i = [], 0
    while i < len(params_text):
        ch = params_text[i]
        if ch in ' ,[{:\r\n\t\f]}':
            i += 1
            continue
        if ch == '"':
            j = i + 1
            while params_text[j] != '"':
                j += 2 if params_text[j] == "\\" else 1
            tokens.append(params_text[i + 1:j])  # raw, exactly as NZBGet stores it
            i = j + 1
        else:
            raise AssertionError(f"non-string token in PPParameters at {i}: {params_text[i:i + 20]!r}")
    assert len(tokens) % 2 == 0, f"odd number of strings: {tokens}"
    return dict(zip(tokens[0::2], tokens[1::2]))


class FakeResponse:
    def raise_for_status(self):
        pass

    def json(self):
        return {"result": 123}


def capture_append(monkeypatch, password: str) -> tuple[dict, str]:
    sent = {}

    def fake_post(url, data=None, headers=None, auth=None, timeout=None):
        sent["body"] = data
        sent["headers"] = headers
        return FakeResponse()
    monkeypatch.setattr(nzbget.requests, "post", fake_post)
    job = nzbget.NzbgetClient("http://nzbget:6789", "u", "p").add_nzb(
        b"<nzb/>", name="Book", category="Audiobooks", password=password)
    assert job == 123
    body = sent["body"].decode("utf-8")
    assert json.loads(body)["method"] == "append"
    # PPParameters is the last argument; hand NZBGet's tokenizer the raw text of it
    return sent, body[body.rindex("[{"):] if password else "[]"


def test_password_reaches_nzbget_as_unpack_password(monkeypatch):
    _, pp_text = capture_append(monkeypatch, "Per.Ardua.Ad.Astra")
    assert nzbget_read_pp_parameters(pp_text) == {"*Unpack:Password": "Per.Ardua.Ad.Astra"}


def test_non_ascii_password_is_sent_unescaped(monkeypatch):
    sent, pp_text = capture_append(monkeypatch, "Café-Größe")
    assert nzbget_read_pp_parameters(pp_text) == {"*Unpack:Password": "Café-Größe"}
    assert "utf-8" in sent["headers"]["Content-Type"]


def test_no_password_sends_no_parameters(monkeypatch):
    sent, _ = capture_append(monkeypatch, "")
    assert json.loads(sent["body"])["params"][-1] == []


def test_parser_port_rejects_the_documented_struct_form():
    # Guard against "fixing" this back to the API-doc form: NZBGet would see
    # parameters named "Name" and "Value" instead of the password.
    parsed = nzbget_read_pp_parameters(json.dumps([{"Name": "*Unpack:Password", "Value": "secret"}]))
    assert "*Unpack:Password" not in parsed
