#!/bin/bash
# Adjust the docker group GID inside the container to match the host's docker socket GID,
# so the jenkins user has permission to talk to /var/run/docker.sock.
set -e

if [ -S /var/run/docker.sock ]; then
    SOCK_GID=$(stat -c '%g' /var/run/docker.sock)
    CURRENT_GID=$(getent group docker | cut -d: -f3 || echo "")
    if [ -n "$SOCK_GID" ] && [ "$SOCK_GID" != "0" ] && [ "$SOCK_GID" != "$CURRENT_GID" ]; then
        groupmod -g "$SOCK_GID" docker 2>/dev/null || true
    fi
    # Ensure jenkins is in the (now correctly-GID'd) docker group
    usermod -aG docker jenkins 2>/dev/null || true
fi

exec su -s /bin/bash jenkins -c "/usr/bin/tini -- /usr/local/bin/jenkins.sh"
