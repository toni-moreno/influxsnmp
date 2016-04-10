package main

import (
	"fmt"
	"log"
	"strings"
	"time"
	//"sync"
	"sync/atomic"

	"github.com/influxdata/influxdb/client/v2"
)

type InfluxConfig struct {
	Host      string `toml:"host"`
	Port      int    `toml:"port"`
	DB        string `toml:"db"`
	User      string `toml:"user"`
	Password  string `toml:"password"`
	Retention string `toml:"retention"`
	iChan     chan *client.BatchPoints
	client    client.Client
	Sent      int64
	Errors    int64
}

func (c *InfluxConfig) incSent() {
	atomic.AddInt64(&c.Sent, 1)
}

func (c *InfluxConfig) addSent(n int64) {
	atomic.AddInt64(&c.Sent, n)
}

func (c *InfluxConfig) incErrors() {
	atomic.AddInt64(&c.Errors, 1)
}

func (c *InfluxConfig) addErrors(n int64) {
	atomic.AddInt64(&c.Errors, n)
}

func (cfg *InfluxConfig) BP() *client.BatchPoints {
	if len(cfg.Retention) == 0 {
		cfg.Retention = "default"
	}
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        cfg.DB,
		RetentionPolicy: cfg.Retention,
		Precision:       "ns", //Default precision for Time lib
	})
	return &bp
}


func (cfg *InfluxConfig) Connect() error {
	/*u, err := url.Parse(fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port))
	if err != nil {
		return err
	}*/

	conf := client.HTTPConfig{
		Addr:     fmt.Sprintf("http://%s:%d", cfg.Host, cfg.Port),
		Username: cfg.User,
		Password: cfg.Password,
	}
	cli, err := client.NewHTTPClient(conf)
	cfg.client = cli
	if err != nil {
		return err
	}

	_, _, err = cfg.client.Ping(time.Duration(5))
	return err
}

func (cfg *InfluxConfig) Init() {

	if verbose {
		log.Println("Connecting to:", cfg.Host)
	}
	cfg.iChan = make(chan *client.BatchPoints, 65535)
	if err := cfg.Connect(); err != nil {
		log.Println("failed connecting to:", cfg.Host)
		log.Println("error:", err)
		log.Fatal(err)
	}
	if verbose {
		log.Println("Connected to:", cfg.Host)
	}

	go influxEmitter(cfg)
}

func (c *InfluxConfig) Send(bps *client.BatchPoints) {
	c.iChan <- bps
}

func (c *InfluxConfig) Hostname() string {
	return strings.Split(c.Host, ":")[0]
}

// use chan as a queue so that interupted connections to
// influxdb server don't drop collected data

func influxEmitter(cfg *InfluxConfig) {
	for {
		select {
		case data := <-cfg.iChan:
			if testing {
				break
			}
			if data == nil {
				log.Println("null influx input")
				continue
			}

			// keep trying until we get it (don't drop the data)
			for {
				if err := cfg.client.Write(*data); err != nil {
					cfg.incErrors()
					log.Println("influxdb write error:", err)
					// try again in a bit
					// TODO: this could be better
					time.Sleep(30 * time.Second)
					continue
				} else {
					cfg.incSent()
				}
				break
			}
		}
	}
}
