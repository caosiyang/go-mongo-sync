/*
   MongoDB synchronizer implemention.
*/
package sync

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go-mongo-sync/utils"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Synchronizer struct {
	config     Config
	srcSession *mgo.Session
	dstSession *mgo.Session
	optime     bson.MongoTimestamp // int64
}

// NewSynchronizer
// - connect
// - get optime
func NewSynchronizer(config Config) *Synchronizer {
	p := new(Synchronizer)
	p.config = config
	if s, err := mgo.DialWithTimeout(p.config.From, time.Second*3); err == nil {
		p.srcSession = s
		p.srcSession.SetSocketTimeout(0)
		p.srcSession.SetSyncTimeout(0)
		p.srcSession.SetMode(mgo.Strong, false)
		p.srcSession.SetCursorTimeout(0)
		fmt.Printf("connected %s\n", p.config.From)
	} else {
		fmt.Println(err, p.config.From)
		return nil
	}
	if s, err := mgo.DialWithTimeout(p.config.To, time.Second*3); err == nil {
		p.dstSession = s
		p.dstSession.SetSocketTimeout(0)
		p.dstSession.SetSyncTimeout(0)
		p.dstSession.SetSafe(&mgo.Safe{W: 1})
		p.dstSession.SetMode(mgo.Eventual, false)
		fmt.Printf("connected %s\n", p.config.To)
	} else {
		fmt.Println(err, p.config.To)
		return nil
	}
	if optime, err := utils.GetOptime(p.srcSession); err == nil {
		p.optime = optime
	} else {
		fmt.Println(err)
		return nil
	}
	fmt.Printf("optime: %v\n", p.optime)
	fmt.Printf("time: %v\n", utils.GetTimeFromOptime(p.optime))
	fmt.Printf("optime: %v\n", utils.GetTimestampFromOptime(p.optime))
	return p
}

func (p *Synchronizer) Run() error {
	if err := p.initialSync(); err != nil {
		return err
	}
	if err := p.oplogSync(); err != nil {
		return err
	}
	return nil
}

func (p *Synchronizer) initialSync() error {
	p.syncDatabases()
	return nil
}

func (p *Synchronizer) oplogSync() error {
	// oplog replayer runs background
	replayer := NewOplogReplayer(p.config.From, p.config.To, p.optime)
	if replayer == nil {
		return errors.New("NewOplogReplayer failed")
	}
	fmt.Println("NewOplogReplayer done")
	replayer.Run()
	return nil
}
func (p *Synchronizer) syncDatabases() error {
	dbnames, err := p.srcSession.DatabaseNames()
	if err != nil {
		return err
	}
	for _, dbname := range dbnames {
		if dbname != "local" && dbname != "admin" {
			if err := p.syncDatabase(dbname); err != nil {
				fmt.Println("sync database:", err)
			}
		}
	}
	return nil
}

func (p *Synchronizer) syncDatabase(dbname string) error {
	collnames, err := p.srcSession.DB(dbname).CollectionNames()
	if err != nil {
		return err
	}
	fmt.Println("sync database:", dbname)
	for _, collname := range collnames {
		// skip collections whose name starts with "system."
		if strings.Index(collname, "system.") == 0 {
			continue
		}
		fmt.Println("\tsync collection:", collname)
		coll := p.srcSession.DB(dbname).C(collname)

		// create indexes
		if indexes, err := coll.Indexes(); err == nil {
			for _, index := range indexes {
				fmt.Println("\t\tcreate index:", index)
				if err := p.dstSession.DB(dbname).C(collname).EnsureIndex(index); err != nil {
					return err
				}
			}
		} else {
			return err
		}

		// sync documents
		n := 0
		c := make(chan bool, 20) // 20 concurrent goroutines
		cursor := coll.Find(nil).Snapshot().Iter()
		for {
			var doc bson.M
			if cursor.Next(&doc) {
				c <- true
				go p.write_document(dbname, collname, doc, c)
				n++
				if n%10000 == 0 {
					fmt.Printf("\t\t%d records done\n", n)
				}
			} else {
				fmt.Printf("\t\t%d records total\n", n)
				break
			}
		}
		if err := cursor.Close(); err != nil {
			fmt.Println("close cursor", err)
			return err
		}
	}
	return nil
}

func (p *Synchronizer) write_document(dbname string, collname string, doc bson.M, c chan bool) error {
	// TODO failover
	defer func() { <-c }()
	if _, err := p.dstSession.Clone().DB(dbname).C(collname).UpsertId(doc["_id"], doc); err != nil {
		fmt.Println("write document:", err)
		return err
	}
	return nil
}
