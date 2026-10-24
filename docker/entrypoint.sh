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
    export HOME=/tmp
    echo "BookClub: running as uid=$PUID gid=$PGID umask=$(umask)"
    exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups -- "$@"
fi

exec "$@"
