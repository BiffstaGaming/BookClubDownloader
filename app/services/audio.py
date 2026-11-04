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
