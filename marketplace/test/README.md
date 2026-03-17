# Online Marketplace Test Project

## Running the project

Order of parameters:

```
1. benchmark [tpcc/marketplace]
2. num_vms_workers [N]
3. num_transaction_workers [N]
4. num_max_transactions_batch [N]
5. batch_window_ms [N]
6. max_time [N]
```

And then run the project with the command as follows (parameters are used as example):
```
java -XX:+UseParallelGC -XX:ParallelGCThreads=1 -Xms16g -Xmx16g -jar target/test-1.0-SNAPSHOT.jar tpcc 1 1 100000 50000000 10000
```

```
java -XX:+AlwaysPreTouch -Xms16g -Xmx16g -Xlog:gc -jar target/test-1.0-SNAPSHOT.jar tpcc 1 1 100000 50000000 10000
```