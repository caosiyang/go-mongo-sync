# go-mongo-sync

An oplog based realtime sync tool for MongoDB written in Go.  
Source should be a replica set and destination could be a mongod/mongos instance or member of replica set.

##  Feature

- initial sync including collections and indexes
- oplog based increment sync
- **concurrent oplog replaying**

## Note

- the following databases are ignored: 'admin', 'local'
- collections that starts with '**system.**' are ignored, e.g.: 'system.users', 'system.profile'

## Usage

    # ./go-mongo-sync --help
    Usage of ./go-mongo-sync:
      -from="": source, a member of replica-set, value should be a hostportstr like 'host:port'
      -to="": destination, a mongos or mongod instance, value should be a hostportstr like 'host:port'
      -direct-connect=false: for source only, disable the automatic replica set server discovery logic and force the use of servers provided only (even if secondaries)
      -ignore-index=false: not create index for collections
      -oplog=false: replay oplog only
      -start-optime=0: start optime, the number of seconds elapsed since January 1 1970 UTC, use this with '--oplog'
      -upsert=false: upsert documents in initial sync, insert documents if not set

## TODO

- support authentication for source and destination
