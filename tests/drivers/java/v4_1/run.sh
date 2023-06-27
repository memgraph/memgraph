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
REACTIVE_STREAM_DEP=reactive-streams.jar

if [ ! -f $DRIVER ]; then
    # Driver downloaded from: http://central.maven.org/maven2/org/neo4j/driver/neo4j-java-driver/1.5.2/neo4j-java-driver-1.5.2.jar
    wget -nv https://repo1.maven.org/maven2/org/neo4j/driver/neo4j-java-driver/4.1.1/neo4j-java-driver-4.1.1.jar -O $DRIVER || exit 1
fi

if [ ! -f $REACTIVE_STREAM_DEP ]; then
    wget -nv https://repo1.maven.org/maven2/org/reactivestreams/reactive-streams/1.0.3/reactive-streams-1.0.3.jar -O $REACTIVE_STREAM_DEP || exit 1
fi

# CentOS 7 doesn't have Java version that supports var keyword
source ../../../../environment/util.sh

if [[ "$( operating_system )" != "centos-7" ]]; then
    javac -classpath .:$DRIVER:$REACTIVE_STREAM_DEP DocsHowToQuery.java
    java -classpath .:$DRIVER:$REACTIVE_STREAM_DEP DocsHowToQuery
fi

javac -classpath .:$DRIVER:$REACTIVE_STREAM_DEP MaxQueryLength.java
java -classpath .:$DRIVER:$REACTIVE_STREAM_DEP MaxQueryLength

javac -classpath .:$DRIVER:$REACTIVE_STREAM_DEP Transactions.java
java -classpath .:$DRIVER:$REACTIVE_STREAM_DEP Transactions

javac -classpath .:$DRIVER:$REACTIVE_STREAM_DEP Metadata.java
java -classpath .:$DRIVER:$REACTIVE_STREAM_DEP Metadata
