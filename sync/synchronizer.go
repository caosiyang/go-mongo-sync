/*
   MongoDB synchronizer implemention.
*/
package sync

import (
	"errors"
	"fmt"
	"log"
	"runtime"
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
//   - connect
//   - get optime
func NewSynchronizer(config Config) *Synchronizer {
	p := new(Synchronizer)
	p.config = config
	if p.config.DirectConnect {
		url := fmt.Sprintf("mongodb://%s/?connect=direct", p.config.From)
		if s, err := mgo.DialWithTimeout(url, time.Second*3); err == nil {
			p.srcSession = s
			p.srcSession.SetSocketTimeout(0)
			p.srcSession.SetSyncTimeout(0)
			p.srcSession.SetMode(mgo.Monotonic, false) // allow to read from secondary
			p.srcSession.SetCursorTimeout(0)
			log.Printf("connected to %s\n", p.config.From)
		} else {
			log.Println(err, p.config.From)
			return nil
		}
	} else {
		if s, err := mgo.DialWithTimeout(p.config.From, time.Second*3); err == nil {
			p.srcSession = s
			p.srcSession.SetSocketTimeout(0)
			p.srcSession.SetSyncTimeout(0)
			p.srcSession.SetMode(mgo.Strong, false) // always read from primary
			p.srcSession.SetCursorTimeout(0)
			log.Printf("connected to %s\n", p.config.From)
		} else {
			log.Println(err, p.config.From)
			return nil
		}
	}
	if s, err := mgo.DialWithTimeout(p.config.To, time.Second*3); err == nil {
		p.dstSession = s
		p.dstSession.SetSocketTimeout(0)
		p.dstSession.SetSyncTimeout(0)
		p.dstSession.SetSafe(&mgo.Safe{W: 1})
		p.dstSession.SetMode(mgo.Eventual, false)
		log.Printf("connected to %s\n", p.config.To)
	} else {
		log.Println(err, p.config.To)
		return nil
	}
	if p.config.StartOptime > 0 {
		p.optime = bson.MongoTimestamp(int64(p.config.StartOptime) << 32)
	} else {
		if optime, err := utils.GetOptime(p.srcSession); err == nil {
			p.optime = optime
		} else {
			log.Println(err)
			return nil
		}
	}
	log.Printf("optime is %v %v\n", utils.GetTimestampFromOptime(p.optime), utils.GetTimeFromOptime(p.optime))
	return p
}

func (p *Synchronizer) Run() error {
	if !p.config.OplogOnly {
		if err := p.initialSync(); err != nil {
			return err
		}
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
				log.Println("sync database:", err)
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
	log.Printf("sync database '%s'\n", dbname)
	for _, collname := range collnames {
		// skip collections whose name starts with "system."
		if strings.Index(collname, "system.") == 0 {
			continue
		}
		log.Printf("\tsync collection '%s.%s'\n", dbname, collname)
		coll := p.srcSession.DB(dbname).C(collname)

		if !p.config.IgnoreIndex {
			// create indexes
			if indexes, err := coll.Indexes(); err == nil {
				for _, index := range indexes {
					log.Println("\t\tcreate index:", index)
					if err := p.dstSession.DB(dbname).C(collname).EnsureIndex(index); err != nil {
						return err
					}
				}
			} else {
				return err
			}
		}

		query := coll.Find(nil)
		total, _ := query.Count()
		if total == 0 {
			log.Println("\t\tskip empty collection")
			continue
		}

		nworkers := runtime.NumCPU()
		channel := make(chan interface{}, 10000)
		done := make(chan struct{}, nworkers)
		for i := 0; i < nworkers; i++ {
			go p.write_document(i, dbname, collname, channel, done)
		}

		n := 0
		cursor := query.Snapshot().Iter()

		for {
			var doc interface{}
			if cursor.Next(&doc) {
				channel <- doc
				n++
				if n%10000 == 0 {
					log.Printf("\t\t%s.%s %d/%d (%.2f%%)\n", dbname, collname, n, total, float64(n)/float64(total)*100)
				}
			} else {
				if err := cursor.Err(); err != nil {
					log.Fatalln("initial sync abort:", err)
				}
				log.Printf("\t\t%s.%s %d/%d (%.2f%%)\n", dbname, collname, n, total, float64(n)/float64(total)*100)
				cursor.Close()
				close(channel)
				break
			}
		}

		// wait for all workers done
		for i := 0; i < nworkers; i++ {
			<-done
		}
	}
	return nil
}

func (p *Synchronizer) write_document(id int, dbname string, collname string, channel <-chan interface{}, done chan<- struct{}) {
	coll := p.dstSession.Clone().DB(dbname).C(collname)
	if p.config.Upsert {
		// upsert
		for doc := range channel {
			if _, err := coll.UpsertId(doc.(bson.M)["_id"], doc); err != nil {
				log.Println("\twrite failed:", err)
			}
		}
	} else {
		// bulk insert
		ndocs := 1000
		docs := make([]interface{}, ndocs)
		i := 0
		for doc := range channel {
			docs[i] = doc
			i++
			if i == ndocs {
				if err := coll.Insert(docs...); err != nil {
					log.Println("\twrite failed:", err)
				}
				i = 0
			}
		}
		if i > 0 {
			for j := 0; j < i; j++ {
				if err := coll.Insert(docs[j]); err != nil {
					log.Println("\twrite failed:", err)
				}
			}
		}
	}
	done <- struct{}{}
}
