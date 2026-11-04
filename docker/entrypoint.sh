#!/bin/sh
# Run BookClub as PUID:PGID (like linuxserver.io images) so the books it writes
# are owned by the same user as Audiobookshelf. Without PUID it runs as root.
set -e

PUID="${PUID:-0}"
PGID="${PGID:-0}"
umask "${UMASK:-002}"

mkdir -p /app/data

if [ "$PUID" != "0" ]; then
    chown -R "$PUID:$PGID" /app/data
    # The default M4B working folder lives in the image; let PUID write to it.
    # (A folder you mount there keeps its own owner.)
    if ! mountpoint -q /m4b 2>/dev/null; then chown "$PUID:$PGID" /m4b 2>/dev/null || true; fi
    export HOME=/tmp
    echo "BookClub: running as uid=$PUID gid=$PGID umask=$(umask)"
    exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups -- "$@"
fi

exec "$@"
