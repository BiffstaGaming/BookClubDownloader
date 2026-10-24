FROM python:3.12-slim

ARG TARGETARCH
ARG M4B_TOOL_VERSION=0.5.2
ARG M4B_TOOL_SHA256=a3ea9b81a4e8ce10ecc85c1fc712c982ba2622f2ed0318fb558dadfad4a590fa
ARG TONE_VERSION=0.2.5
ARG TONE_SHA256_AMD64=acc626328d6230045f7032cec1a7f87139645691fae6a367897a8cab67c20d3e
ARG TONE_SHA256_ARM64=9b1c3064d2a0ed7b57e430b31dfec9d744524d62f9af1de562e516dc8aa24c23

WORKDIR /app

# PHP CLI for m4b-tool, ffmpeg for encoding, curl for downloads and the health check
RUN apt-get update && apt-get install -y --no-install-recommends \
    php-cli \
    php-mbstring \
    php-xml \
    php-curl \
    php-zip \
    ffmpeg \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# mp4v2-utils (which provides mp4info) is not in Debian Bookworm repos.
# Create a shim using ffprobe as a fallback for duration detection; tone (below)
# is preferred by m4b-tool whenever it is installed.
RUN printf '#!/bin/sh\n\
DURATION=$(ffprobe -v quiet -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "$1" 2>/dev/null)\n\
[ -z "$DURATION" ] && { echo "mp4info: cannot read $1" >&2; exit 1; }\n\
python3 -c "\n\
d = float('"'"'$DURATION'"'"')\n\
print('"'"'Track\\tType\\tInfo'"'"')\n\
print('"'"'1\\taudio\\tMPEG-4 AAC LC, {:.3f} secs, 128 kbps, 44100 Hz'"'"'.format(d))\n\
"\n' > /usr/local/bin/mp4info && chmod +x /usr/local/bin/mp4info

# m4b-tool, pinned and checksum-verified
RUN curl -fsSL "https://github.com/sandreas/m4b-tool/releases/download/v${M4B_TOOL_VERSION}/m4b-tool.phar" \
        -o /usr/local/bin/m4b-tool.phar \
    && echo "${M4B_TOOL_SHA256}  /usr/local/bin/m4b-tool.phar" | sha256sum -c - \
    && printf '#!/bin/sh\nexec php /usr/local/bin/m4b-tool.phar "$@"\n' > /usr/local/bin/m4b-tool \
    && chmod +x /usr/local/bin/m4b-tool

# tone — m4b-tool uses it to write tags, chapters and cover art into the .m4b
# (without it or mp4v2, tagging silently fails). Self-contained .NET binary.
RUN case "${TARGETARCH:-amd64}" in \
        amd64) TONE_ARCH=linux-x64;   TONE_SHA256="${TONE_SHA256_AMD64}" ;; \
        arm64) TONE_ARCH=linux-arm64; TONE_SHA256="${TONE_SHA256_ARM64}" ;; \
        *) echo "Unsupported architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && curl -fsSL "https://github.com/sandreas/tone/releases/download/v${TONE_VERSION}/tone-${TONE_VERSION}-${TONE_ARCH}.tar.gz" \
        -o /tmp/tone.tar.gz \
    && echo "${TONE_SHA256}  /tmp/tone.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/tone.tar.gz -C /tmp \
    && install -m 0755 "/tmp/tone-${TONE_VERSION}-${TONE_ARCH}/tone" /usr/local/bin/tone \
    && rm -rf /tmp/tone*

# tone does not need ICU culture data; skip the libicu dependency
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 \
    PYTHONUNBUFFERED=1

RUN tone -v && m4b-tool --version && command -v setpriv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /app/data && chmod +x /app/docker/entrypoint.sh

EXPOSE 9933
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -fsS http://localhost:9933/api/health || exit 1

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9933"]
