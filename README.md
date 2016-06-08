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

## Dependency

- mgo

## Usage

    # ./go-mongo-sync --help
    Usage of go-mongo-sync:
      -from="": source, should be a member of replica-set
      -oplog=false: replay oplog only
      -to="": destination, should be a mongos or mongod instance

## TODO

- support authentication for source and destination
- specify startOptime in '-oplog' mode
