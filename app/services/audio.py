"""
What's in the downloaded audio (codec, length) and how hard to run the encoder.

Encoding is the slow, hot part of the pipeline. The biggest savings, in order:
  1. Don't encode at all: files that are already AAC are joined as they are.
  2. Encode less: mono 22.05 kHz is about a quarter of the work of 44.1 kHz stereo.
  3. Run gently: low priority, and optionally only on some of the CPU cores, so
     the machine stays responsive and cooler (at the cost of taking longer).
"""
import json
import logging
import os
import shutil
import subprocess

logger = logging.getLogger(__name__)

AAC_EXTENSIONS = (".m4a", ".m4b", ".mp4", ".aac")


def probe(path: str) -> dict:
    """{"codec", "sample_rate", "channels", "seconds"} for an audio file ({} if ffprobe can't read it)."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a:0",
             "-show_entries", "stream=codec_name,sample_rate,channels:format=duration",
             "-of", "json", path],
            capture_output=True, text=True, errors="replace", timeout=60,
        )
        data = json.loads(result.stdout or "{}")
    except Exception:  # not installed, unreadable file, timeout — the length is just unknown
        return {}
    if not isinstance(data, dict):
        return {}
    stream = (data.get("streams") or [{}])[0]
    try:
        seconds = float((data.get("format") or {}).get("duration") or 0)
    except ValueError:
        seconds = 0.0
    return {
        "codec": stream.get("codec_name") or "",
        "sample_rate": int(stream.get("sample_rate") or 0),
        "channels": int(stream.get("channels") or 0),
        "seconds": seconds,
    }


def probe_all(files: list[str]) -> list[dict]:
    return [probe(f) for f in files]


def total_minutes(probes: list[dict]) -> float:
    return round(sum(p.get("seconds") or 0 for p in probes) / 60, 1)


def can_join_without_encoding(files: list[str], probes: list[dict]) -> bool:
    """
    True when every file is AAC with the same sample rate and channel count, so
    the parts can be joined as they are (seconds instead of many minutes).
    Mixed formats would glitch at the joins, so those are still encoded.
    """
    if not files or len(files) != len(probes):
        return False
    if not all(f.lower().endswith(AAC_EXTENSIONS) for f in files):
        return False
    if not all(p.get("codec") == "aac" for p in probes):
        return False
    return len({(p.get("sample_rate"), p.get("channels")) for p in probes}) == 1


def available_cores() -> int:
    return len(allowed_cpus())


def effective_cores(setting: str) -> int:
    """The core limit from Settings (blank or 0 = no limit), never more than the machine has."""
    try:
        wanted = int(setting or 0)
    except ValueError:
        wanted = 0
    return min(wanted, available_cores()) if wanted > 0 else 0


def gentle(cmd: list[str], max_cores: int) -> list[str]:
    """
    Run `cmd` at low priority and, if `max_cores` is set, only on that many cores.
    ffmpeg processes started by m4b-tool inherit both.
    """
    if os.name == "nt":
        return cmd
    prefix = ["nice", "-n", "10"] if shutil.which("nice") else []
    if max_cores and shutil.which("taskset"):
        prefix += ["taskset", "-c", ",".join(str(c) for c in allowed_cpus()[:max_cores])]
    return prefix + cmd


def allowed_cpus() -> list[int]:
    """CPU numbers this container may use (Docker can pin it to e.g. cores 4-7)."""
    try:
        return sorted(os.sched_getaffinity(0))
    except AttributeError:
        return list(range(os.cpu_count() or 1))


def describe_speed(audio_seconds: float, elapsed_seconds: float) -> str:
    """'11h 32m of audio in 3m 10s (218× real time)'."""
    def fmt(seconds: float) -> str:
        seconds = int(seconds)
        h, rem = divmod(seconds, 3600)
        m, s = divmod(rem, 60)
        return f"{h}h {m:02d}m" if h else f"{m}m {s:02d}s"
    if not audio_seconds or not elapsed_seconds:
        return f"in {fmt(elapsed_seconds)}"
    return f"{fmt(audio_seconds)} of audio in {fmt(elapsed_seconds)} ({audio_seconds / elapsed_seconds:.0f}× real time)"


# ── when encoding may run: a time window, and a CPU temperature limit ────────

# hwmon drivers that report the CPU itself (preferred over board/ACPI sensors)
CPU_SENSORS = ("coretemp", "k10temp", "zenpower", "cpu_thermal", "cpu-thermal", "soc_thermal", "x86_pkg_temp")
FALLBACK_SENSORS = ("acpitz",)
COOL_DOWN_DEGREES = 5  # a paused encode resumes this far below the limit


def _read_number(path: str) -> float | None:
    try:
        with open(path, encoding="ascii", errors="replace") as f:
            return float(f.read().strip())
    except (OSError, ValueError):
        return None


def _read_text(path: str) -> str:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            return f.read().strip()
    except OSError:
        return ""


def cpu_temperature(sys_root: str = "/sys/class") -> float | None:
    """The hottest CPU sensor in °C, or None when the container can't see one."""
    import glob

    found: dict[str, list[float]] = {"cpu": [], "fallback": []}
    for hw in glob.glob(os.path.join(sys_root, "hwmon", "hwmon*")):
        name = _read_text(os.path.join(hw, "name"))
        group = "cpu" if name in CPU_SENSORS else "fallback" if name in FALLBACK_SENSORS else ""
        if not group:
            continue
        for f in glob.glob(os.path.join(hw, "temp*_input")):
            value = _read_number(f)
            if value:
                found[group].append(value / 1000)
    for zone in glob.glob(os.path.join(sys_root, "thermal", "thermal_zone*")):
        kind = _read_text(os.path.join(zone, "type"))
        group = "cpu" if kind in CPU_SENSORS else "fallback" if kind in FALLBACK_SENSORS else ""
        value = _read_number(os.path.join(zone, "temp")) if group else None
        if value:
            found[group].append(value / 1000)
    readings = found["cpu"] or found["fallback"]
    return round(max(readings), 1) if readings else None


def _parse_hhmm(text: str):
    from datetime import time as dtime

    try:
        hours, minutes = (int(part) for part in (text or "").strip().split(":"))
        return dtime(hours, minutes)
    except (ValueError, TypeError):
        return None


def local_now(timezone_name: str):
    """Now in the user's time zone (Settings), else the container's."""
    from datetime import datetime

    if timezone_name:
        try:
            from zoneinfo import ZoneInfo
            return datetime.now(ZoneInfo(timezone_name)).replace(tzinfo=None)
        except Exception:
            logger.warning("Unknown time zone %r — using the container's clock", timezone_name)
    return datetime.now()


def in_window(now_time, start: str, end: str) -> bool:
    """True when `now_time` is inside start–end (HH:MM). Either blank = any time. Spans midnight if end < start."""
    begin, finish = _parse_hhmm(start), _parse_hhmm(end)
    if begin is None or finish is None or begin == finish:
        return True
    if begin < finish:
        return begin <= now_time < finish
    return now_time >= begin or now_time < finish


def encode_gate(settings: dict, temperature: float | None = None, now_time=None) -> str:
    """
    "" when encoding may start now, else why not. `settings` holds encode_window_start,
    encode_window_end, encode_max_temp and timezone (as strings).
    """
    start, end = settings.get("encode_window_start", ""), settings.get("encode_window_end", "")
    if now_time is None:
        now_time = local_now(settings.get("timezone", "")).time()
    if not in_window(now_time, start, end):
        return f"waiting for the encoding hours ({start}–{end})"
    limit = _read_limit(settings.get("encode_max_temp", ""))
    if limit:
        temperature = cpu_temperature() if temperature is None else temperature
        if temperature is not None and temperature >= limit:
            return f"CPU is {temperature:.0f}°C (limit {limit:.0f}°C) — waiting for it to cool down"
    return ""


def _read_limit(text: str) -> float | None:
    try:
        value = float(str(text).strip())
    except ValueError:
        return None
    return value if value > 0 else None


def temperature_limit(text: str) -> float | None:
    return _read_limit(text)


def restrictions_set(settings: dict) -> bool:
    window = _parse_hhmm(settings.get("encode_window_start", "")) and _parse_hhmm(settings.get("encode_window_end", ""))
    return bool(window) or bool(_read_limit(settings.get("encode_max_temp", "")))
