!/bin/bash -e
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

cd $DIR/../../build


# Start the first process
echo "Starting memgraph..."
./memgraph --log-level=TRACE --storage-recover-on-startup=true --storage-snapshot-interval-sec=600 --log-file=$DIR/memgraph.logs --bolt-port 7687 &

sleep 5
# Capture the process ID (PID)
memgraph=$!

echo $memgraph

function cleanup {
    echo "Terminating child processes..."
    echo "Stopping memgraph..."
    kill -9 $memgraph
    sleep 5
    echo "Script execution completed."
    exit
}


trap cleanup SIGINT

# Function to execute query commands
execute_query() {
    local query=$1
    echo "$query" | mgconsole
}

previous_time=$(date +%s)
previous_minute=$(date +%M)
previous_hour=$(date +%H)

while true; do
    current_time=$(date +%s)
    current_minute=$(date +%M)
    current_hour=$(date +%H)

    if ((current_time - previous_time >= 5)); then
        echo "Five seconds have passe running create query."
        execute_query "Create (:Node)-[:CONNECTED]->(:Node);" &
        previous_time=$current_time
    fi

    if [[ $current_minute != $previous_minute ]]; then
        echo "One minute has passed running match query."

        execute_query "MATCH (n)-[r]->[m] RETURN DISTINCT n;" &
        previous_minute=$current_minute
    fi

    if [[ $current_hour != $previous_hour ]]; then
        echo "One hour has passed running delete query."
        execute_query "MATCH (node) DETACH DELETE node;" &
        previous_hour=$current_hour
    fi

done
