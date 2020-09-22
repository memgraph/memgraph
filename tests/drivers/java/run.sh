#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

for i in java javac; do
    if ! which $i >/dev/null; then
        echo "Please install $i!"
        exit 1
    fi
done

DRIVER=neo4j-java-driver.jar

if [ ! -f $DRIVER ]; then
    # Driver downloaded from: http://central.maven.org/maven2/org/neo4j/driver/neo4j-java-driver/1.5.2/neo4j-java-driver-1.5.2.jar
    wget -nv https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/drivers/java/neo4j-java-driver-1.5.2.jar -O $DRIVER || exit 1
fi

javac -classpath .:$DRIVER Basic.java
java -classpath .:$DRIVER Basic

javac -classpath .:$DRIVER MaxQueryLength.java
java -classpath .:$DRIVER MaxQueryLength
