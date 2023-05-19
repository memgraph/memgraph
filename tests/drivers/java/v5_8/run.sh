#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

for i in java mvn; do
    if ! which $i >/dev/null; then
        echo "Please install $i!"
        exit 1
    fi
done

JAVA_VER=$(java -version 2>&1 >/dev/null | grep 'version' | cut -d "\"" -f2 | cut -d "." -f1)
if [ $JAVA_VER -ne 17 ]
then
    echo "neo4j-java-driver v5.8 requires Java 17. Please install it!"
    exit 1
fi

# CentOS 7 doesn't have Java version that supports var keyword
source ../../../../environment/util.sh

mvn clean package

java -jar target/DocsHowToQuery.jar
java -jar target/MaxQueryLength.jar
java -jar target/Transactions.jar
