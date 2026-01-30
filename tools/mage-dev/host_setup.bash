#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Running host setup..."
# NOTE: Feel free to tweak the below code to what's required for you tests.

sudo sysctl -w vm.max_map_count=1048576

# Copy the desired module, convenient to change the mage code and just run it
# under the container (reload of module/modules required).
echo "$SCRIPT_DIR"
cp $SCRIPT_DIR/../../python/embed_worker/embed_worker.py $SCRIPT_DIR/
cp $SCRIPT_DIR/../../python/embeddings.py $SCRIPT_DIR/

if [ ! "$(docker ps -q -f name=embeddings)" ]; then
  docker run --name=embeddings -d --rm --gpus=all --network host -p 7687:7687 \
    -v $SCRIPT_DIR:/app memgraph/memgraph-mage:3.5.1-cuda \
    --schema-info-enabled=True --telemetry-enabled=False \
    --also-log-to-stderr --log-level=TRACE
fi
# The idea here is to run docker once, under subsequent restarts of this
# script, just the modules will be swapped -> only the modules reload required.
docker exec -it -u root embeddings bash -c "/app/container_setup.bash"

sleep 10
cat "$SCRIPT_DIR/../../../dataset.cypherl" | grep -v "MATCH" | mgconsole -host 127.0.0.1 -port 7687
echo "call mg.load_all();" | mgconsole -host 127.0.0.1 -port 7687

docker logs --follow embeddings
