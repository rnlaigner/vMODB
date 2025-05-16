#!/bin/bash

start_process() {
    local jar_path=$1
    local server=$2
    echo "Starting $server server..."
    java --enable-preview \
         --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
         --add-opens java.base/jdk.internal.util=ALL-UNNAMED \
         -jar "$jar_path" > "$server.log" 2>&1 &
    echo $! > "${server}.pid"
}

start_process "tpcc/inventory-tpcc/target/inventory-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar" "inventory"
start_process "tpcc/order-tpcc/target/order-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar" "order"
start_process "tpcc/warehouse-tpcc/target/warehouse-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar" "warehouse"

sleep 3

echo "Starting proxy server..."
java --enable-preview \
     --add-exports java.base/jdk.internal.misc=ALL-UNNAMED \
     --add-opens java.base/jdk.internal.util=ALL-UNNAMED \
     -jar tpcc/proxy-tpcc/target/proxy-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar

cleanup() {
    echo "Cleaning up background processes..."
    for pid_file in *.pid; do
        if [ -f "$pid_file" ]; then
            kill $(cat "$pid_file") 2>/dev/null
            rm "$pid_file"
        fi
    done
}

trap cleanup EXIT 