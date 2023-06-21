!/bin/bash -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

cd $DIR/../../build


query_list=(
    "CREATE (:Node)-[:CONNECTED]->(:Node);"
    "CREATE (:Node)-[:CONNECTED]->(:Node);"
    "CREATE (:Node)-[:CONNECTED]->(:Node);"
    "CREATE (:Node)-[:CONNECTED]->(:Node);"
    "CREATE (:Node)-[:CONNECTED]->(:Node);"
    "CREATE (:Node)-[:CONNECTED]->(:Node);"
    "CREATE (:Node)-[:CONNECTED]->(:Node);"
    "CREATE (:Node)-[:CONNECTED]->(:Node);"
    "CREATE (:Node)-[:CONNECTED]->(:Node);"
    "CREATE (:Node)-[:CONNECTED]->(:Node);"
    "CALL dummy_query.dummy_read() YIELD *;"
    "CALL dummy_query.dummy_write() YIELD *;"
    "CREATE (:Node)-[:CONNECTED]->(:Node);"
    "CREATE (:Node)-[:CONNECTED]->(:Node);"

)

# Start the first memgraph instance
echo "Starting memgraph..."
./memgraph --log-level=TRACE --storage-recover-on-startup=true --storage-snapshot-interval-sec=600 --log-file=$DIR/memgraph.log --bolt-port 7687 --query-modules-directory=$DIR/query_modules &

sleep 5
# Capture the process ID (PID)
memgraph=$!
echo $memgraph

docker run -d -p 7688:7687 --name memgraph_docker memgraph/memgraph:2.8.0 --log-level=TRACE --storage-recover-on-startup=true --storage-snapshot-interval-sec=600 &
sleep 5

cd $DIR/query_modules
tar -cf - dummy_query.py | docker cp -a - memgraph_docker:/usr/lib/memgraph/query_modules/
docker exec -it -u 0 memgraph_docker bash -c "chown -R root:root /usr/lib/memgraph/query_modules"
docker_ip=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' memgraph_docker)
echo $docker_ip

#check docker query module ownership
docker exec -it memgraph_docker ls -l /usr/lib/memgraph/query_modules


# Function to execute query commands
execute_query_native() {
    local query=$1

    echo "$query" | mgconsole -host "127.0.0.1" -port 7687
}

execute_query_docker(){
    local query=$1
    echo "$query" | mgconsole -host "127.0.0.1" -port 7688
}

execute_query_docker "CALL mg.load_all();"
execute_query_docker "CALL dummy_query.dummy_read() YIELD *;"

function cleanup {
    echo "Terminating child processes..."
    echo "Stopping memgraph..."
    kill -9 $memgraph
    sleep 5
    echo "Stopping memgraph_docker..."
    docker stop memgraph_docker
    echo "Removing memgraph_docker..."
    docker rm memgraph_docker
    echo "Script execution completed."
    exit
}


trap cleanup SIGINT




previous_time=$(date +%s)
previous_minute=$(date +%M)
previous_hour=$(date +%H)

while true; do
    current_time=$(date +%s)
    current_minute=$(date +%M)
    current_hour=$(date +%H)

    if ((current_time - previous_time >= 5)); then
        echo "Five seconds have passe running create query."
        for query in "${query_list[@]}"; do
            execute_query_native "$query" &
            execute_query_docker "$query" &
        done
        previous_time=$current_time
    fi

    if [[ $current_minute != $previous_minute ]]; then
        echo "One minute has passed running match query."

        execute_query_native "MATCH (n)-[r]->(m) RETURN DISTINCT COUNT(n);" &
        execute_query_docker "MATCH (n)-[r]->(m) RETURN DISTINCT COUNT(n);" &
        previous_minute=$current_minute
    fi

    if [[ $current_hour != $previous_hour ]]; then
        echo "One hour has passed running delete query."
        execute_query_native "MATCH (n) DETACH DELETE n;" &
        execute_query_docker "MATCH (n) DETACH DELETE n;" &
        previous_hour=$current_hour
    fi

done
