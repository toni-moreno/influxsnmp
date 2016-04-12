package main

import (
	"flag"
	"fmt"
	//"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kardianos/osext"
	"github.com/spf13/viper"
)

const layout = "2006-01-02 15:04:05"

type GeneralConfig struct {
	LogDir string `toml:"logdir"`
}

var (
	quit          = make(chan struct{})
	verbose       bool
	startTime     = time.Now()
	showConfig    bool
	repeat        = 0
	freq          = 30
	httpPort      = 8080
	oidToName     = make(map[string]string)
	nameToOid     = make(map[string]string)
	appdir, _     = osext.ExecutableFolder()
	logDir        = filepath.Join(appdir, "log")
	configFile    = filepath.Join(appdir, "config.toml")
	errorLog      *os.File
	errorDuration = time.Duration(10 * time.Minute)
	errorPeriod   = errorDuration.String()
	errorMax      = 100
	errorName     string

	cfg = struct {
		Selfmon      SelfMonConfig
		Metrics      map[string]*SnmpMetricCfg
		Measurements map[string]*InfluxMeasurementCfg
		GetGroups    map[string]*MGroupsCfg
		SnmpDevice   map[string]*SnmpDeviceCfg
		Influx       map[string]*InfluxConfig
		HTTP         HTTPConfig
		General      GeneralConfig
	}{}
)

func fatal(v ...interface{}) {
	log.SetOutput(os.Stderr)
	log.Fatalln(v...)
}

func spew(x ...interface{}) {
	if verbose {
		fmt.Println(x...)
	}
}

func flags() *flag.FlagSet {
	var f flag.FlagSet
	f.BoolVar(&showConfig, "showconf", showConfig, "show all devices config and exit")
	f.StringVar(&configFile, "config", configFile, "config file")
	f.BoolVar(&verbose, "verbose", verbose, "verbose mode")
	f.IntVar(&freq, "freq", freq, "delay (in seconds)")
	f.IntVar(&httpPort, "http", httpPort, "http port")
	f.StringVar(&logDir, "logs", logDir, "log directory")
	f.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		f.VisitAll(func(flag *flag.Flag) {
			format := "%10s: %s\n"
			fmt.Fprintf(os.Stderr, format, "-"+flag.Name, flag.Usage)
		})
		fmt.Fprintf(os.Stderr, "\nAll settings can be set in config file: %s\n", configFile)
		os.Exit(1)

	}
	return &f
}

/*
init_metrics_cfg this function does 2 things
1.- Initialice id from key of maps for all SnmpMetricCfg and InfluxMeasurementCfg objects
2.- Initialice references between InfluxMeasurementCfg and SnmpMetricGfg objects

*/

func init_metrics_cfg() error {
	//Initialize references to SnmpMetricGfg into InfluxMeasurementCfg
	log.Println("--------------------Initializing Config metrics-------------------")
	log.Println("Initializing SNMPMetricconfig...")
	for m_key, m_val := range cfg.Metrics {
		err := m_val.Init(m_key)
		if err != nil {
			log.Println("Error in Metric config:", err)
			//if some error int the format the metric is deleted from the config
			delete(cfg.Metrics, m_key)
		}
	}
	log.Println("Initializing MEASSUREMENTSconfig...")
	for m_key, m_val := range cfg.Measurements {
		err := m_val.Init(m_key, &cfg.Metrics)
		if err != nil {
			log.Println("Error in Metric config:", err)
			//if some error int the format the metric is deleted from the config
			delete(cfg.Metrics, m_key)
		}

		log.Printf("FIELDMETRICS: %+v", m_val.fieldMetric)
	}
	log.Println("-----------------------END Config metrics----------------------")
	return nil
}

func init() {
	// parse first time to see if config file is being specified
	f := flags()
	f.Parse(os.Args[1:])
	// now load up config settings
	if _, err := os.Stat(configFile); err == nil {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("config")
		viper.AddConfigPath("/opt/influxsnmp/conf/")
		viper.AddConfigPath("./conf/")
		viper.AddConfigPath(".")
	}
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	err = viper.Unmarshal(&cfg)
	if err != nil {
		panic(fmt.Errorf("unable to decode into struct, %v \n", err))
	}
	//Debug	fmt.Printf("%+v\n", cfg)
	if len(cfg.General.LogDir) > 0 {
		logDir = cfg.General.LogDir
	}

	init_metrics_cfg()

	for _, s := range cfg.SnmpDevice {
		s.debugging = make(chan bool)
		s.enabled = make(chan chan bool)
	}
	var ok bool
	for k, c := range cfg.SnmpDevice {
		//Inticialize each SNMP device
		c.Init(k)
		if c.Freq == 0 {
			c.Freq = freq
		}
	}

	// only run when one needs to see the interface names of the device
	if showConfig {
		for _, c := range cfg.SnmpDevice {
			fmt.Println("\nSNMP host:", c.id)
			fmt.Println("=========================================")
			c.printConfig()
		}
		os.Exit(0)
	}

	// re-read cmd line args to override as indicated
	f = flags()
	f.Parse(os.Args[1:])
	os.Mkdir(logDir, 0755)

	//construc Extra tag map => Traspasar a SnmpDeviceCfg.Init()
	for name, c := range cfg.SnmpDevice {
		if len(c.ExtraTags) > 0 {
			c.TagMap = make(map[string]string)
			for _, tag := range c.ExtraTags {
				s := strings.Split(tag, "=")
				key, value := s[0], s[1]
				c.TagMap[key] = value
			}
		} else {
			fmt.Printf("No map detected in %s\n", name)
		}
		//Debug fmt.Printf("TAG ARRAY[ %s ]: %+v\n", name, c.ExtraTags)
		//Debug fmt.Printf("EXTRA TAGS MAP[ %s ]: %+v\n", name, c.TagMap)
	}

	// now make sure each snmp device has a db
	for name, c := range cfg.SnmpDevice {
		// default is to use name of snmp config, but it can be overridden
		if len(c.Config) > 0 {
			name = c.Config
		}
		if c.Influx, ok = cfg.Influx[name]; !ok {
			if c.Influx, ok = cfg.Influx["*"]; !ok {
				fatal("No influx config for snmp device:", name)
			}
		}
		c.Influx.Init()
	}
	//make sure the selfmon has a deb
	if cfg.Selfmon.Enabled {
		cfg.Selfmon.Init()
		cfg.Selfmon.Influx, ok = cfg.Influx["*"]
		cfg.Selfmon.Influx.Init()
		fmt.Printf("SELFMON enabled %+vn\n", cfg.Selfmon)
	} else {
		fmt.Printf("SELFMON disabled %+vn\n", cfg.Selfmon)
	}

	var ferr error
	errorName = fmt.Sprintf("error.%d.log", cfg.HTTP.Port)
	errorPath := filepath.Join(logDir, errorName)
	errorLog, ferr = os.OpenFile(errorPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0664)
	if ferr != nil {
		log.Fatal("Can't open error log:", ferr)
	}
}

func errLog(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg, args...)
	fmt.Fprintf(errorLog, msg, args...)
}

func errMsg(msg string, err error) {
	now := time.Now()
	errLog("%s\t%s: %s\n", now.Format(layout), msg, err)
}

func main() {
	var wg sync.WaitGroup
	defer func() {
		errorLog.Close()
	}()
	if cfg.Selfmon.Enabled {
		cfg.Selfmon.ReportStats(&wg)
	}

	for _, c := range cfg.SnmpDevice {
		wg.Add(1)
		go c.Gather(&wg)
	}

	var port int
	if cfg.HTTP.Port > 0 {
		port = cfg.HTTP.Port
	} else {
		port = httpPort
	}

	if port > 0 {
		webServer(port)
	} else {
		<-quit
	}
}
