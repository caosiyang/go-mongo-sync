package sync

import (
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"go-mongo-sync/utils"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

// oplog replay worker
type Worker struct {
	hostportstr string
	session     *mgo.Session
	oplogChan   chan bson.M
	mutex       sync.Mutex
	optime      bson.MongoTimestamp
	nOplog      uint64
	nDone       uint64
	id          int
}

func NewWorker(hostportstr string, id int) *Worker {
	p := new(Worker)
	p.hostportstr = hostportstr
	p.oplogChan = make(chan bson.M, 100)
	s, err := mgo.Dial(p.hostportstr)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	p.session = s
	p.session.SetSafe(&mgo.Safe{W: 1})
	p.id = id
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
	//if p.nOplog%1000 == 0 {
	//	fmt.Println("goroutine", p.id, p.nOplog)
	//}
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
				fmt.Println("LastError", err)
			default:
				switch err.Error() {
				case "not found":
					//fmt.Println("Error", err)
				case "EOF": // reconnect
					fmt.Println("Error", err, oplog)
					p.session.Close()
					p.session = utils.Reconnect(p.hostportstr)
					goto Begin
				default:
					fmt.Println("Unknown Error", err)
					p.session.Close()
					p.session = utils.Reconnect(p.hostportstr)
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
	oplogChan  chan bson.M
	srcSession *mgo.Session
	nWorkers   int
	workers    [16]*Worker // TODO slice?
}

func NewOplogReplayer(src string, dst string, optime bson.MongoTimestamp) *OplogReplayer {
	p := new(OplogReplayer)
	p.src = src
	p.dst = dst
	p.optime = optime
	p.oplogChan = make(chan bson.M)
	if s, err := mgo.Dial(p.src); err == nil {
		p.srcSession = s
	} else {
		return nil
	}
	p.nWorkers = 8
	if p.nWorkers > 16 {
		return nil
	}
	for i := 0; i < p.nWorkers; i++ {
		worker := NewWorker(p.dst, i)
		if worker != nil {
			p.workers[i] = worker
			go worker.Run()
		} else {
			return nil
		}
	}
	return p
}

// dispatch oplog to workers
func (p *OplogReplayer) Run() error {
	cursor := p.srcSession.DB("local").C("oplog.rs").Find(bson.M{"ts": bson.M{"$gte": p.optime}}).Tail(-1)
	n := 0
	for {
		var oplog bson.M
		if cursor.Next(&oplog) {
			// [command] should excute until all previous operations done to guatantee sequence
			// worker-0 is the master goroutine, all commands will be sent to it
			// [insert/update/delete] hash to different workers
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
					fmt.Println("ERROR", err)
					continue
				}
				bytes, err := bson.Marshal(bson.M{"_id": oid})
				if err != nil {
					fmt.Println("ERROR oid to bytes", err)
					continue
				}
				wid := crc32.ChecksumIEEE(bytes) % uint32(p.nWorkers)
				p.workers[wid].Push(oplog)
			}
			n += 1
			if n%1000 == 0 {
				var optime bson.MongoTimestamp
				optime = (1 << 63) - 1
				//fmt.Println("max", optime)
				for i := 0; i < p.nWorkers; i++ {
					tmp := p.workers[i].Optime()
					if tmp < optime {
						optime = tmp
					}
					//fmt.Println(i, tmp)
				}
				//fmt.Println("done", optime)
				p.optime = optime
				fmt.Println(time.Now(), n, utils.GetTimeFromOptime(p.optime), utils.GetTimestampFromOptime(p.optime))
			}
		}
	}
	return nil
}

// push oplog
func (p *OplogReplayer) Push(oplog bson.M) {
	p.oplogChan <- oplog
}
