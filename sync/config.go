package sync

import (
	"errors"
	"flag"
	"log"
	"strconv"
	"strings"
)

// Config implemention that parse from command arguments.
type Config struct {
	From        string
	To          string
	SrcHost     string
	SrcPort     int
	DstHost     string
	DstPort     int
	StartOptime int
	OplogOnly   bool
	//Log         string
}

// load and parse command-line flags
func (p *Config) Load() error {
	flag.StringVar(&p.From, "from", "", "source, should be a member of replica-set")
	flag.StringVar(&p.To, "to", "", "destination, should be a mongos or mongod instance")
	flag.BoolVar(&p.OplogOnly, "oplog", false, "replay oplog only")
	flag.Parse()
	if err := p.validate(); err != nil {
		return err
	}
	p.print()
	return nil
}

// validate command-line flags
func (p *Config) validate() error {
	var err error
	p.SrcHost, p.SrcPort, err = parse_host_port(p.From)
	if err != nil {
		return errors.New("from error: " + err.Error())
	}
	p.DstHost, p.DstPort, err = parse_host_port(p.To)
	if err != nil {
		return errors.New("to error: " + err.Error())
	}
	return nil
}

// print config
func (p *Config) print() {
	log.Printf("from: %s:%d\n", p.SrcHost, p.SrcPort)
	log.Printf("to:   %s:%d\n", p.DstHost, p.DstPort)
	if p.OplogOnly {
		log.Println("oplogOnly: true")
	}
}

// parse hostportstr
func parse_host_port(hostportstr string) (host string, port int, err error) {
	s := strings.Split(hostportstr, ":")
	if len(s) != 2 {
		return host, port, errors.New("invalid hostportstr")
	}
	host = s[0]
	port, err = strconv.Atoi(s[1])
	if err != nil {
		return host, port, errors.New("invalid port")
	}
	if port < 0 || port > 65535 {
		return host, port, errors.New("invalid port")
	}
	return host, port, nil
}
