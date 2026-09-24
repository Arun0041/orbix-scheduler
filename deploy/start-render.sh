#!/bin/sh

# Start Etcd in the background
echo "Starting Etcd..."
etcd --name render-etcd \
     --data-dir /data/etcd \
     --listen-client-urls http://127.0.0.1:2379 \
     --advertise-client-urls http://127.0.0.1:2379 &

# Give Etcd a couple of seconds to initialize
sleep 3

# Configure and start Orbix
echo "Starting Orbix Scheduler..."
export ORBIX_ETCD=http://127.0.0.1:2379
export ORBIX_NODE_ID=render-portfolio-node
export ORBIX_DATA_DIR=/data/orbix
export ORBIX_LISTEN=:8080

# Execute Orbix in the foreground so the container stays alive
exec orbixd
