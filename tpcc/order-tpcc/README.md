# TPC-C Order

This project is responsible to execute the order service of vMODB for the TPC-C benchmark.

## Running the project

If you are in this project's root folder, run the project with the following command:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar target/order-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar
```

If you are outside the vms-runtime-java folder, run:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar vms-runtime-java/tpcc/order-tpcc/target/order-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar
```

If you are inside the vms-runtime-java folder, run:
```
java --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar tpcc/order-tpcc/target/order-tpcc-1.0-SNAPSHOT-jar-with-dependencies.jar
```

To collect GC diagnostics:
```
java '-Xlog:gc*,safepoint,classhisto=trace:file=gc+classhisto*' -XX:+UnlockDiagnosticVMOptions --enable-preview --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/jdk.internal.util=ALL-UNNAMED -jar tpcc/proxy-tpcc/target/proxy-tpcc-1.0-SNAPSHOT.jar 2
```