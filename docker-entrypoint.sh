#!/bin/bash
set -e

# Make the docker socket accessible to the jenkins user.
# Running as root here so we can chmod before handing off.
if [ -S /var/run/docker.sock ]; then
    chmod 666 /var/run/docker.sock
fi

exec su -s /bin/bash jenkins -c "exec /usr/bin/tini -- /usr/local/bin/jenkins.sh"
