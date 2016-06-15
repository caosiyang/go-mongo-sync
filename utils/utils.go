/*
   Mongo helper.
*/
package utils

import (
	"errors"
	"log"
	"strings"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Timestamp struct {
	sec int64 // seconds since January 1, 1970 UTC
	inc int64 // the incrementing counter
}

type ReplicaSetStatus struct {
	Set     string             `bson:"set"`
	MyState int                `bson:"myState"`
	Members []ReplicaSetMember `bson:"members"`
	Ok      int                `bson:"ok"`
	Errmsg  string             `bson:"errmsg"`
}

type ReplicaSetMember struct {
	Name     string              `bson:"name"`
	State    int                 `bson:"state"`
	StateStr string              `bson:"stateStr"`
	Optime   bson.MongoTimestamp `bson:"optime"`
	Self     bool                `bson:"self"`
}

// retry until connect
func Reconnect(hostportstr string) *mgo.Session {
	for {
		log.Println("reconnect", hostportstr)
		if s, err := mgo.DialWithTimeout(hostportstr, time.Second*3); err == nil {
			return s
		}
	}
}

// get current optime of PRIMARY in the replica set
func GetOptime(s *mgo.Session) (optime bson.MongoTimestamp, err error) {
	var res ReplicaSetStatus
	s.DB("admin").Run("replSetGetStatus", &res)
	if res.Ok == 1 {
		for _, member := range res.Members {
			if member.State == 1 {
				return member.Optime, nil
			}
		}
		return optime, errors.New("It's a replica set, but not found PRIMARY")
	} else {
		return optime, errors.New(res.Errmsg)
	}
}

// get time from optime
func GetTimeFromOptime(optime bson.MongoTimestamp) time.Time {
	return time.Unix(int64(optime)>>32, 0)
}

// get timestamp from optime
func GetTimestampFromOptime(optime bson.MongoTimestamp) Timestamp {
	sec := int64(optime) >> 32
	inc := int64(optime) & 0x00000000FFFFFFFF
	return Timestamp{sec, inc}
}

// get ObjectID from oplog
func GetObjectIdFromOplog(oplog bson.M) (oid interface{}, err error) {
	op := oplog["op"]
	switch op {
	case "i":
		return oplog["o"].(bson.M)["_id"], nil
	case "u":
		return oplog["o2"].(bson.M)["_id"], nil
	case "d":
		return oplog["o"].(bson.M)["_id"], nil
	}
	return bson.NewObjectId(), errors.New("get oid failed")
}

// replay oplog
func ReplayOplog(session *mgo.Session, oplog bson.M) error {
	op := oplog["op"]
	ns := oplog["ns"]
	switch op {
	case "i": // insert
		dbname := strings.Split(ns.(string), ".")[0]
		collname := strings.Split(ns.(string), ".")[1]
		//id := oplog["o"].(bson.M)["_id"]
		//if _, err := session.DB(dbname).C(collname).UpsertId(id, oplog["o"]); err != nil {
		if err := session.DB(dbname).C(collname).Insert(oplog["o"]); err != nil {
			//log.Println("insert", err)
			return err
		}
	case "u": // update
		dbname := strings.Split(ns.(string), ".")[0]
		collname := strings.Split(ns.(string), ".")[1]
		if err := session.DB(dbname).C(collname).Update(oplog["o2"], oplog["o"]); err != nil {
			//log.Println("update", err)
			return err
		}
	case "d": // delete
		dbname := strings.Split(ns.(string), ".")[0]
		collname := strings.Split(ns.(string), ".")[1]
		if err := session.DB(dbname).C(collname).Remove(oplog["o"]); err != nil {
			//log.Println("delete", err)
			return err
		}
	case "c": // command
		dbname := strings.Split(ns.(string), ".")[0]
		if err := session.DB(dbname).Run(oplog["o"], nil); err != nil {
			//log.Println("command", err)
			return err
		}
	case "n": // no-op
		// do nothing
	default:
	}
	return nil
}
