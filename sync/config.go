package sync

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
)

// Config implemention that parse from command arguments.
type Config struct {
	From        string
	To          string
	Database    string
	Query       string
	Log         string
	StartOptime int
	SrcHost     string
	SrcPort     int
	DstHost     string
	DstPort     int
}

// load and parse command-line flags
func (p *Config) Load() error {
	flag.StringVar(&p.From, "from", "", "source, should be a member of replica-set")
	flag.StringVar(&p.To, "to", "", "destination, should be a mongos or mongod instance")
	flag.StringVar(&p.Database, "db", "", "database to sync")
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
	fmt.Printf("from: %s:%d\n", p.SrcHost, p.SrcPort)
	fmt.Printf("to:   %s:%d\n", p.DstHost, p.DstPort)
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
