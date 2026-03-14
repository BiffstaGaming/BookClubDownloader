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
    mp4v2-utils \
    curl \
    && rm -rf /var/lib/apt/lists/*

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
