#!/bin/sh

if [ $# -eq 0 ]; then
    echo "Please choose target: "
    echo "     ./build.sh [ build | clean ]"
    exit 1
fi

if [ $1 == "clean" ]; then
    echo "cleaning..."
    rm -rf files
    rm *.class
else

    javac MapReduceServer.java
    javac MapReduceWorker.java
    javac WordCountMapper.java
    javac WordCountReducer.java

    mkdir -p files/bin
    cp WordCountMapper.class files/bin
    cp WordCountReducer.class files/bin
fi
