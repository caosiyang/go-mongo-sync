package sync

import (
	"hash/crc32"
	"log"
	"runtime"
	"sync"
	"time"

	"go-mongo-sync/utils"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// oplog replay worker
type Worker struct {
	id          int
	hostportstr string
	session     *mgo.Session
	oplogChan   chan bson.M
	mutex       sync.Mutex
	optime      bson.MongoTimestamp
	nOplog      uint64
	nDone       uint64
}

func NewWorker(hostportstr string, id int) *Worker {
	p := new(Worker)
	p.id = id
	p.hostportstr = hostportstr
	p.oplogChan = make(chan bson.M, 100)
	if s, err := mgo.Dial(p.hostportstr); err != nil {
		log.Println(err)
		return nil
	} else {
		p.session = s
		p.session.SetSafe(&mgo.Safe{W: 1})
		p.session.SetSocketTimeout(0)
		p.session.SetSyncTimeout(0)
	}
	return p
}

// oplog queue size
func (p *Worker) Qsize() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.nOplog - p.nDone
}

// optime
func (p *Worker) Optime() bson.MongoTimestamp {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.optime
}

// push oplog
func (p *Worker) Push(oplog bson.M) {
	p.oplogChan <- oplog
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.nOplog++
}

// replay oplog
func (p *Worker) Run() error {
	for {
		oplog := <-p.oplogChan
	Begin:
		if err := utils.ReplayOplog(p.session, oplog); err != nil {
			// TODO
			switch err.(type) {
			case *mgo.LastError:
				log.Println("LastError", err)
			default:
				switch err.Error() {
				case "not found":
					//log.Println("Error", err)
				case "EOF": // reconnect
					log.Println("Error", err, oplog)
					p.session.Close()
					p.session = utils.Reconnect(p.hostportstr)
					p.session.SetSocketTimeout(0)
					p.session.SetSyncTimeout(0)
					goto Begin
				default:
					log.Println("Unknown Error", err)
					p.session.Close()
					p.session = utils.Reconnect(p.hostportstr)
					p.session.SetSocketTimeout(0)
					p.session.SetSyncTimeout(0)
					goto Begin
				}
			}
		}
		p.mutex.Lock()
		p.nDone++
		p.optime = oplog["ts"].(bson.MongoTimestamp)
		p.mutex.Unlock()
	}
}

// oplog replayer
type OplogReplayer struct {
	src        string
	dst        string
	optime     bson.MongoTimestamp
	srcSession *mgo.Session
	nWorkers   int
	workers    [32]*Worker // TODO slice?
}

func NewOplogReplayer(src string, dst string, optime bson.MongoTimestamp) *OplogReplayer {
	p := new(OplogReplayer)
	p.src = src
	p.dst = dst
	p.optime = optime
	if s, err := mgo.Dial(p.src); err == nil {
		p.srcSession = s
		p.srcSession.SetSocketTimeout(0) // locating oplog may be slow
	} else {
		return nil
	}
	p.nWorkers = runtime.NumCPU()
	if p.nWorkers > 32 {
		p.nWorkers = 32
	}
	for i := 0; i < p.nWorkers; i++ {
		worker := NewWorker(p.dst, i)
		if worker != nil {
			p.workers[i] = worker
			go worker.Run() // concurrent oplog replaying
		} else {
			return nil
		}
	}
	return p
}

// dispatch oplog to workers
func (p *OplogReplayer) Run() error {
	log.Printf("locating oplog at %v\n", utils.GetTimestampFromOptime(p.optime))
Begin:
	iter := p.srcSession.DB("local").C("oplog.rs").Find(bson.M{"ts": bson.M{"$gte": p.optime}}).Tail(-1)
	n := 0
	oplog_valid := false

	if inc := int64(p.optime) << 32 >> 32; inc == 0 {
		oplog_valid = true
		log.Print("start optime specified by user, skip verification")
	}

	for {
		var oplog bson.M
		if iter.Next(&oplog) {
			if !oplog_valid {
				if oplog["ts"] != p.optime {
					log.Fatalf("oplog is stale, except %v, current %v\n",
						utils.GetTimestampFromOptime(p.optime),
						utils.GetTimestampFromOptime(oplog["ts"].(bson.MongoTimestamp)))
				}
				oplog_valid = true
				log.Print("oplog is OK")
				continue
			}
			// **COMMAND** should excute until all previous operations done to guatantee sequence
			// worker-0 is the master goroutine, all commands will be sent to it
			// INSERT/UPDATE/DELETE hash to different workers
			switch oplog["op"] {
			case "c":
				// wait for all previous operations done
				for {
					ready := true
					for i := 0; i < p.nWorkers; i++ {
						if p.workers[i].Qsize() > 0 {
							ready = false
							break
						}
					}
					if ready {
						break
					} else {
						time.Sleep(time.Millisecond * 10) // sleep 10ms
					}
				}
				p.workers[0].Push(oplog)
				// wait for command done
				for {
					if p.workers[0].Qsize() == 0 {
						break
					} else {
						time.Sleep(time.Millisecond * 10) // sleep 10ms
					}
				}
			case "i":
				fallthrough
			case "u":
				fallthrough
			case "d":
				oid, err := utils.GetObjectIdFromOplog(oplog)
				if err != nil {
					log.Fatal("FATAL GetObjectIdFromOplog", err)
					continue
				}
				bytes, err := bson.Marshal(bson.M{"_id": oid})
				if err != nil {
					log.Fatal("FATAL oid to bytes", err)
					continue
				}
				wid := crc32.ChecksumIEEE(bytes) % uint32(p.nWorkers)
				p.workers[wid].Push(oplog)
			}
			n += 1
			if n%1000 == 0 {
				// get optime of the lastest oplog has been replayed
				var optime bson.MongoTimestamp = 0
				//optime = (1 << 63) - 1
				for i := 0; i < p.nWorkers; i++ {
					ts := p.workers[i].Optime()
					if optime < ts {
						optime = ts
					}
				}
				p.optime = optime
				log.Printf("\t%d replayed, %d secs delay, sync to %v %v",
					n,
					time.Now().Unix()-utils.GetTimeFromOptime(p.optime).Unix(),
					utils.GetTimeFromOptime(p.optime),
					utils.GetTimestampFromOptime(p.optime))
			}
		} else {
			if err := iter.Err(); err != nil {
				log.Print("tail oplog failed:", err)
				switch err.Error() {
				case "EOF":
					p.srcSession = utils.Reconnect(p.src)
					p.srcSession.SetSocketTimeout(0) // locating oplog may be slow
					goto Begin
				default:
					log.Fatal("unknown exception:", err)
				}
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
	if err := iter.Close(); err != nil {
		log.Fatal("kill cursor failed:", err)
	}
	return nil
}
