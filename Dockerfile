FROM python:3.12-slim

WORKDIR /app

# Install PHP CLI, ffmpeg, and curl for m4b-tool
RUN apt-get update && apt-get install -y --no-install-recommends \
    php-cli \
    php-mbstring \
    php-xml \
    php-curl \
    php-zip \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# mp4v2-utils (which provides mp4info) is not in Debian Bookworm repos.
# Create a shim using ffprobe so m4b-tool can detect file lengths.
RUN printf '#!/bin/sh\n\
DURATION=$(ffprobe -v quiet -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "$1" 2>/dev/null)\n\
[ -z "$DURATION" ] && { echo "mp4info: cannot read $1" >&2; exit 1; }\n\
python3 -c "\n\
d = float('"'"'$DURATION'"'"')\n\
h = int(d // 3600)\n\
m = int((d %% 3600) // 60)\n\
s = d %% 60\n\
print('"'"'Track\\tType\\tInfo'"'"')\n\
print('"'"'Duration: {}:{:02d}:{:06.3f}, 1 audio track'"'"'.format(h, m, s))\n\
"\n' > /usr/local/bin/mp4info && chmod +x /usr/local/bin/mp4info

# Download m4b-tool and create a wrapper script
RUN curl -L https://github.com/sandreas/m4b-tool/releases/latest/download/m4b-tool.phar \
    -o /usr/local/bin/m4b-tool.phar \
    && chmod +x /usr/local/bin/m4b-tool.phar \
    && echo '#!/bin/sh\nexec php /usr/local/bin/m4b-tool.phar "$@"' > /usr/local/bin/m4b-tool \
    && chmod +x /usr/local/bin/m4b-tool

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /app/data

EXPOSE 9933
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9933"]
