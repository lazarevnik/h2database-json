#!/bin/sh
dir=$(dirname "$0")
java -cp "$dir/h2.jar:$H2DRIVERS:$CLASSPATH:$dir/../ext/jackson-core-2.9.5.jar:$dir/../ext/jackson-databind-2.9.5.jar:$dir/../ext/jackson-annotations-2.9.5.jar" org.h2.tools.Console "$@"
