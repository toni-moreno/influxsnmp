package main

import (
	"fmt"
	//"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/soniah/gosnmp"
)

type SnmpDeviceCfg struct {
	id string
	//snmp connection config
	Host    string `toml:"host"`
	Port    int    `toml:"port"`
	Retries int    `toml:"retries"`
	Timeout int    `toml:"timeout"`
	Repeat  int    `toml:"repeat"`
	//snmp auth  config
	SnmpVersion string `toml:"snmpversion"`
	Community   string `toml:"community"`
	V3SecLevel  string `toml:"v3seclevel"`
	V3AuthUser  string `toml:"v3authuser"`
	V3AuthPass  string `toml:"v3authpass"`
	V3AuthProt  string `toml:"v3authprot"`
	V3PrivPass  string `toml:"v3privpass"`
	V3PrivProt  string `toml:"v3privprot"`
	//snmp runtime config
	Freq     int    `toml:"freq"`
	PortFile string `toml:"portfile"`
	Config   string `toml:"config"`
	Debug    bool   `toml:"debug"`
	//influx tags
	ExtraTags []string `toml:"extra-tags"`
	TagMap    map[string]string
	//Filters for measurements
	MetricGroups []string   `toml:"metricgroups"`
	MeasFilters  [][]string `toml:"measfilters"`

	//Measurments array

	InfmeasArray []*InfluxMeasurement

	//SNMP and Influx Clients config
	snmpClient *gosnmp.GoSNMP
	Influx     *InfluxConfig
	LastError  time.Time
	//Runtime stats
	Requests int64
	Gets     int64
	Errors   int64
	//runtime controls
	debugging chan bool
	enabled   chan chan bool
}

/*
Init  does the following

- allocate and fill array for all measurements defined for this device
- for each indexed measurement  load device labels from IndexedOID and fiter them if defined measurement filters.
- Initialice each SnmpMetric from each measuremet.
*/

func (c *SnmpDeviceCfg) Init(name string) {
	c.id = name
	//Init SNMP client device
	client, err := snmpClient(c)
	if err != nil {
		fatal("Client connect error:", err)
	}
	/*	defer func() {
		client.Conn.Close()
	}()*/
	c.snmpClient = client

	//Alloc array
	c.InfmeasArray = make([]*InfluxMeasurement, 0, 0)
	log.Printf("-----------------Init device %s------------------", c.Host)
	//for this device get MetricGroups and search all measurements

	//log.Printf("SNMPDEV: %+v", c)
	for _, dev_meas := range c.MetricGroups {

		//Selecting all Metric Groups that matches with device.MetricGroups

		selGroups := make(map[string]*MGroupsCfg, 0)
		var RegExp = regexp.MustCompile(dev_meas)
		for key, val := range cfg.GetGroups {
			if RegExp.MatchString(key) {
				selGroups[key] = val
			}

		}

		log.Printf("SELECTED GROUPS: %+v", selGroups)

		//Only For selected Groups we will get all selected measurements and we will remove repeated values

		selMeas := make([]string, 0)
		for key, val := range selGroups {
			log.Println("Selecting from group", key)
			for _, item := range val.Measurements {
				log.Println("Selecting measurements", item, "from group", key)
				selMeas = append(selMeas, item)
			}
		}

		selMeasUniq := removeDuplicatesUnordered(selMeas)

		//Now we know what measurements names  will send influx from this device

		log.Println("DEVICE MEASUREMENT: ", dev_meas, "HOST: ", c.Host)
		for _, val := range selMeasUniq {
			//check if measurement exist
			if m_val, ok := cfg.Measurements[val]; !ok {
				log.Println("WARNING no measurement configured with name ", val, "in host :", c.Host)
			} else {

				log.Println("MEASUREMENT CFG KEY:", val, " VALUE ", m_val.Name)
				imeas := &InfluxMeasurement{
					cfg: m_val,
					//lastValue: make([][]int, 0),
					//lastTime:  make([][]time.Time, 0),
				}
				c.InfmeasArray = append(c.InfmeasArray, imeas)
			}
		}
	}

	/*For each Indexed measurement
	a) LoadLabels for all device available tags
	b) apply filters , and get list of names Indexed tames for add to IndexTAG

	Filter format
	-------------
	F[0] 	= measurement name
	F[1] 	= FilterType ( know values "file" , "OIDCondition" )
	F[2] 	= Filename ( if F[1] = file)
		= OIDname for condition ( if[F1] = OIDCondition )
	F[3]  = Condition Type ( if[F1] = OIDCondition ) (known values "eq","lt","gt")
	F[4]  = Value for condition
	*/

	for _, m := range c.InfmeasArray {
		//loading all posible values.
		if m.cfg.GetMode == "indexed" {
			log.Println("Cargando Index names in :", m.cfg.id)
			m.loadIndexedLabels(c)
		}
		//loading filters
		log.Println("Buscando filtro para  ", m.cfg.id)
		var flt string
		for _, f := range c.MeasFilters {
			if f[0] == m.cfg.id {
				log.Println("Se ha entontrado filtro ", m.cfg.id, "tipo", f[1])
				if m.cfg.GetMode == "indexed" {
					flt = f[1]
					//OK we can apply filters
					switch {
					case flt == "file":
						enable := f[3] == "EnableAlias"
						m.Filter = &MeasFilterCfg{
							fType:       flt,
							FileName:    f[2],
							enableAlias: enable,
						}
						m.applyFileFilter(m.Filter.FileName, enable)
					case flt == "OIDCondition":
						m.Filter = &MeasFilterCfg{
							fType:     flt,
							OIDCond:   f[2],
							condType:  f[3],
							condValue: f[4],
						}

						m.applyOIDCondFilter(c,
							m.Filter.OIDCond,
							m.Filter.condType,
							m.Filter.condValue)
					default:
						log.Println("ERROR: Invalid GetMode Type:", flt)
					}

				} else {
					//no filters enabled  on not indexed measurements
					log.Println("Filters not enabled on not indexed measurements")
				}

			} else {
				log.Println("No se ha encontrado filtro para measurement:", m.cfg.id)
			}
		}
		//Loading final Values to query with snmp
		if len(c.MeasFilters) > 0 {
			m.filterIndexedLabels(flt)
		} else {
			m.IndexedLabels()
		}
		log.Printf("MEASUREMENT HOST:%s | %s | %+v\n", c.Host, m.cfg.id, m)
	}
	//Initialize all snmpMetrics  objects and OID array
	for _, m := range c.InfmeasArray {
		m.values = make(map[string]map[string]*SnmpMetric)

		//create metrics.
		switch m.cfg.GetMode {
		case "value":
			//for each field
			idx := make(map[string]*SnmpMetric)
			for _, smcfg := range m.cfg.fieldMetric {
				log.Println("initializing [value]metric cfg", smcfg.id)
				metric := &SnmpMetric{cfg: smcfg, realOID: smcfg.BaseOID}
				metric.Init()
				idx[smcfg.id] = metric
			}
			m.values["0"] = idx

		case "indexed":
			//for each field an each index (previously initialized)
			for key, label := range m.CurIndexedLabels {
				idx := make(map[string]*SnmpMetric)
				log.Printf("initializing [indexed] metric cfg for [%s/%s]", key, label)
				for _, smcfg := range m.cfg.fieldMetric {
					metric := &SnmpMetric{cfg: smcfg, realOID: smcfg.BaseOID + "." + key}
					metric.Init()
					idx[smcfg.id] = metric
				}
				m.values[label] = idx
			}

		default:
			log.Println("Measurement GetMode Config ERROR:", m.cfg.GetMode)
		}
		log.Printf("DEBUG ARRAY VALUES for host %s :%s : %+v", c.Host, m.cfg.Name, m.values)
		//building real OID array for SNMPWALK and OID=> snmpMetric map to asign results to each object
		m.snmpOids = []string{}
		m.oidSnmpMap = make(map[string]*SnmpMetric)
		//metric level
		for k_idx, v_idx := range m.values {
			log.Println("KEY iDX", k_idx)
			//index level
			for k_m, v_m := range v_idx {
				log.Println("KEY METRIC", k_m, "OID", v_m.realOID)
				m.snmpOids = append(m.snmpOids, v_m.realOID)
				m.oidSnmpMap[v_m.realOID] = v_m

			}
		}
		//		log.Printf("DEBUG oid map %+v", m.oidSnmpMap)

	}
	//get data first time
	// useful to inicialize counter all value and test device snmp availability
	for _, m := range c.InfmeasArray {
		if m.cfg.GetMode == "value" || c.SnmpVersion == "1" {
			_, _, err := m.SnmpGetData(c.snmpClient)
			if err != nil {
				fatal("SNMP First Get Data error for host", c.Host)
			}
		} else {
			_, _, err := m.SnmpBulkData(c.snmpClient)
			if err != nil {
				fatal("SNMP First Get Data error for host", c.Host)
			}

		}

	}

}

func (c *SnmpDeviceCfg) printConfig() {

	fmt.Printf("Host: %s Port: %d Version: %s\n", c.Host, c.Port, c.SnmpVersion)
	fmt.Printf("----------------------------------------------\n")
	for _, v_m := range c.InfmeasArray {
		fmt.Printf(" Measurement : %s\n", v_m.cfg.id)
		fmt.Printf(" ----------------------------------------------------------\n")
		v_m.printConfig()
	}
}

func (c *SnmpDeviceCfg) DebugAction() string {
	debug := make(chan bool)
	c.enabled <- debug
	if <-debug {
		return "disable"
	}
	return "enable"
}

func (c *SnmpDeviceCfg) incRequests() {
	atomic.AddInt64(&c.Requests, 1)
}

func (c *SnmpDeviceCfg) addRequests(n int64) {
	atomic.AddInt64(&c.Requests, n)
}
func (c *SnmpDeviceCfg) incGets() {
	atomic.AddInt64(&c.Gets, 1)
}
func (c *SnmpDeviceCfg) addGets(n int64) {
	atomic.AddInt64(&c.Gets, 1)
}

func (c *SnmpDeviceCfg) incErrors() {
	atomic.AddInt64(&c.Errors, 1)
}

func (c *SnmpDeviceCfg) addErrors(n int64) {
	atomic.AddInt64(&c.Errors, n)
}

func (s *SnmpDeviceCfg) DebugLog() *log.Logger {
	name := filepath.Join(logDir, "debug_"+strings.Replace(s.Host, ".", "-", -1)+".log")
	if l, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0664); err == nil {
		return log.New(l, "", 0)
	} else {
		fmt.Fprintln(os.Stderr, err)
		return nil
	}
}

func (s *SnmpDeviceCfg) Gather(wg *sync.WaitGroup) {
	client := s.snmpClient
	debug := false

	log.Printf("Beginning gather process for device %s (%s)", s.id, s.Host)

	client = s.snmpClient
	c := time.Tick(time.Duration(s.Freq) * time.Second)
	for {
		time.Sleep(time.Duration(s.Timeout) * time.Second)
		bpts := s.Influx.BP()

		for _, m := range s.InfmeasArray {
			log.Println("----------------Processing measurement :", m.cfg.id)
			if m.cfg.GetMode == "value" || s.SnmpVersion == "1" {
				n_gets, n_errors, _ := m.SnmpGetData(client)
				if n_gets > 0 {
					s.addGets(n_gets)
				}
				if n_errors > 0 {
					s.addErrors(n_errors)
				}
			} else {
				n_gets, n_errors, _ := m.SnmpBulkData(client)
				if n_gets > 0 {
					s.addGets(n_gets)
				}
				if n_errors > 0 {
					s.addErrors(n_errors)
				}

			}
			//prepare batchpoint and
			points := m.GetInfluxPoint(s.Host, s.TagMap)
			(*bpts).AddPoints(points)

		}
		s.Influx.Send(bpts)

		// pause for interval period and have optional debug toggling
	LOOP:
		for {
			select {
			case <-c:
				break LOOP
			case debug := <-s.debugging:
				log.Println("debugging:", debug)
				if debug && client.Logger == nil {
					client.Logger = s.DebugLog()
				} else {
					client.Logger = nil
				}
			case status := <-s.enabled:
				status <- debug
			}
		}
	}
	wg.Done()
}
