#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$DIR"

if [ -d "/usr/lib/jvm/java-17-oracle" ]; then
  export JAVA_HOME="/usr/lib/jvm/java-17-oracle"
fi
if [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
  export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
fi
if [ -d "/opt/apache-maven-3.9.3" ]; then
  export M2_HOME="/opt/apache-maven-3.9.3"
fi
export PATH="$JAVA_HOME/bin:$M2_HOME/bin:$PATH"

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
