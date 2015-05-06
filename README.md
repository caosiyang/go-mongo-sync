# go-mongo-sync

MongoDB数据同步工具，支持实时同步。
与python-mongo-sync相比，go-mongo-sync在确保数据一致性的同时，提供同步效率。
oplog并发回放机制，提高回放oplog的效率，即使在源端写操作频繁的情况下也能保证实时同步。
目前仅支持一个MongoDB复制集的全量数据同步。

## 用法

    # ./go-mongo-sync --help
    Usage of ./go-mongo-sync:
      -db="": database to sync
      -from="": source, should be a member of replica-set
      -to="": destination, should be a mongos or mongod instance
