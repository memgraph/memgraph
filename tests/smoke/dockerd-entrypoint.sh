#!/usr/bin/env bash
set -e

# If the daemon.json doesn't exist, you can create a sensible default
if [ ! -f /etc/docker/daemon.json ]; then
  mkdir -p /etc/docker
  cat <<EOF >/etc/docker/daemon.json
{
  "data-root": "/var/lib/docker"
}
EOF
fi

# Start the Docker daemon in foreground, listening on the Unix socket
exec dockerd \
  --host=unix:///var/run/docker.sock \
  --storage-driver=vfs
