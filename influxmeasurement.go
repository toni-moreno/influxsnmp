package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	//	"math/big"
	"path/filepath"
	"strings"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/soniah/gosnmp"
)

type InfluxMeasurementCfg struct {
	id          string   //name of the key in the config array
	Name        string   `toml:"name"`
	Fields      []string `toml:"fields"`
	GetMode     string   `toml:"getmode"` //0=value 1=indexed
	IndexOID    string   `toml:"indexoid"`
	IndexTag    string   `toml:"indextag"`
	fieldMetric []*SnmpMetricCfg
}

func (m *InfluxMeasurementCfg) Init(name string, MetricCfg *map[string]*SnmpMetricCfg) error {
	m.id = name
	//validate config values
	if len(m.Name) == 0 {
		return errors.New("Name not set in measurement Config " + m.id)
	}
	if len(m.Fields) == 0 {
		return errors.New("No Fields added to measurement " + m.id)
	}

	switch m.GetMode {
	case "indexed":
		if len(m.IndexOID) == 0 {
			return errors.New("Indexed measurement with no IndexOID in measurement Config " + m.id)
		}
		if len(m.IndexTag) == 0 {
			return errors.New("Indexed measurement with no IndexTag configuredin measurement " + m.id)
		}
		if !strings.HasPrefix(m.IndexOID, ".") {
			return errors.New("Bad BaseOid format:" + m.IndexOID + " in metric Config " + m.id)
		}

	case "value":
	default:
		return errors.New("Unknown GetMode" + m.GetMode + " in measurement Config " + m.id)
	}

	log.Println("processing measurement key: ", name)
	log.Printf("%+v", m)
	for _, f_val := range m.Fields {
		log.Println("looking for measure ", m.Name, " fields: ", f_val)
		if val, ok := (*MetricCfg)[f_val]; ok {
			log.Println("Found ok!")
			//map is correct
			m.fieldMetric = append(m.fieldMetric, val)
		} else {
			log.Println("WARNING measurement field ", f_val, " NOT FOUND !")
		}
	}
	//check if fieldMetric
	if len(m.fieldMetric) == 0 {
		var s string
		for _, v := range m.Fields {
			s += v
		}
		return errors.New("No metrics found with names" + s + " in measurement Config " + m.id)
	}
	return nil
}

type MeasFilterCfg struct {
	fType       string //file/OidCondition
	FileName    string
	enableAlias bool
	OIDCond     string
	condType    string
	condValue   string
}

type InfluxMeasurement struct {
	cfg              *InfluxMeasurementCfg
	values           map[string]map[string]*SnmpMetric //snmpMetric mapped with metric_names and Index
	snmpOids         []string
	oidSnmpMap       map[string]*SnmpMetric //snmpMetric mapped with real OID's
	Filterlabels     map[string]string
	AllIndexedLabels map[string]string //all available values on the remote device
	CurIndexedLabels map[string]string
	Filter           *MeasFilterCfg
}

func (m *InfluxMeasurement) printConfig() {
	if m.Filter != nil {
		switch m.Filter.fType {
		case "file":
			fmt.Printf(" ----------------------------------------------------------\n")
			fmt.Printf(" File Filter: %s ( EnableAlias: %b)\n", m.Filter.FileName, m.Filter.enableAlias)
			fmt.Printf(" ----------------------------------------------------------\n")
		case "OIDCondition":
			fmt.Printf(" ----------------------------------------------------------\n")
			fmt.Printf(" OID Condition Filter: %s ( [%s] %s)\n", m.Filter.OIDCond, m.Filter.condType, m.Filter.condValue)
			fmt.Printf(" ----------------------------------------------------------\n")
		}

	}
	for _, v := range m.cfg.fieldMetric {
		fmt.Printf("\t*Metric[%s]\tName[%s]\tOID:%s\t(%s) \n", v.id, v.FieldName, v.BaseOID, v.DataSrcType)
	}
	if m.cfg.GetMode == "indexed" {
		fmt.Printf(" ---------------------------------------------------------\n")
		for k, v := range m.CurIndexedLabels {
			fmt.Printf("\t\tIndex[%s / %s]\n", k, v)
		}
	}
}

func (m *InfluxMeasurement) GetInfluxPoint(host string, hostTags map[string]string) []*client.Point {
	var ptarray []*client.Point
	FullTags := map[string]string{
		"host": host,
	}
	//adding host specific tabs
	for key, value := range hostTags {
		FullTags[key] = value
	}

	switch m.cfg.GetMode {
	case "value":
		k := m.values["0"]
		var t time.Time
		Fields := make(map[string]interface{})
		for _, v_mtr := range k {
			log.Println("generating field for ", v_mtr.cfg.FieldName, "value", v_mtr.cookedValue)
			log.Printf("DEBUG METRIC %+v", v_mtr)
			Fields[v_mtr.cfg.FieldName] = v_mtr.cookedValue
			t = v_mtr.curTime
		}
		log.Printf("FIELDS:%+v", Fields)
		log.Printf("TAGS:%+v", FullTags)

		pt, err := client.NewPoint(
			m.cfg.Name,
			FullTags,
			Fields,
			t,
		)
		if err != nil {
			log.Printf("WARNING  error in influx point building:%s", err)
		} else {
			log.Printf("DEBUG INFLUX POINT[%s] value: %+v", m.cfg.Name, pt)
			ptarray = append(ptarray, pt)
		}

	case "indexed":
		var t time.Time
		for k_idx, v_idx := range m.values {
			log.Println("generating influx point for indexed", k_idx)
			//copy tags and add index tag
			Tags := make(map[string]string)
			for k_t, v_t := range FullTags {
				Tags[k_t] = v_t
			}
			Tags[m.cfg.IndexTag] = k_idx
			log.Printf("IDX :%+v", v_idx)
			Fields := make(map[string]interface{})
			for _, v_mtr := range v_idx {
				log.Printf("DEBUG METRIC %+v", v_mtr.cfg)
				log.Println("generating field for Metric", v_mtr.cfg.FieldName)
				Fields[v_mtr.cfg.FieldName] = v_mtr.cookedValue
				t = v_mtr.curTime

			}
			log.Printf("FIELDS:%+v", Fields)
			log.Printf("TAGS:%+v", Tags)
			pt, err := client.NewPoint(
				m.cfg.Name,
				Tags,
				Fields,
				t,
			)
			if err != nil {
				log.Printf("WARNING  error in influx point building:%s", err)
			} else {

				log.Printf("DEBUG INFLUX POINT[%s] index [%s]: %+v", m.cfg.Name, k_idx, pt)
				ptarray = append(ptarray, pt)
			}
		}

	}

	return ptarray

}

/*
SnmpBulkData GetSNMP Data
*/

func (m *InfluxMeasurement) SnmpBulkData(snmp *gosnmp.GoSNMP) (int64, int64, error) {

	now := time.Now()
	var sent int64 = 0
	var errs int64 = 0

	setRawData := func(pdu gosnmp.SnmpPDU) error {
		log.Printf("DEBUG pdu:%+v", pdu)
		if pdu.Value == nil {
			log.Printf("WARNING : no value retured by pdu :%+v", pdu)
		}
		if metric, ok := m.oidSnmpMap[pdu.Name]; ok {
			log.Println("OK measurement ", m.cfg.id, "SNMP RESULT OID", pdu.Name, "MetricFound", pdu.Value)
			//val := pdu.Value
			metric.setRawData(pduVal2Int64(pdu), now)

		} else {
			log.Println("ERROR OID", pdu.Name, "Not Found in measurement", m.cfg.id)
		}
		return nil
	}
	for _, v := range m.cfg.fieldMetric {
		if err := snmp.BulkWalk(v.BaseOID, setRawData); err != nil {
			log.Printf("selected OID %s", v.BaseOID)
			errLog("SNMP (%s) get error: %s\n", snmp.Target, err)
			errs++
		}
		sent++
	}

	return sent, errs, nil
}

/*
GetSnmpData GetSNMP Data
*/

func (m *InfluxMeasurement) SnmpGetData(snmp *gosnmp.GoSNMP) (int64, int64, error) {

	now := time.Now()
	var sent int64 = 0
	var errs int64 = 0
	l := len(m.snmpOids)
	for i := 0; i < l; i += maxOids {
		end := i + maxOids
		if end > l {
			end = len(m.snmpOids)
		}
		log.Printf("DEBUG GET SNMP DATA FROM %d to %d", i, end)
		//	log.Printf("DEBUG oids:%+v", m.snmpOids)
		//	log.Printf("DEBUG oidmap:%+v", m.oidSnmpMap)
		pkt, err := snmp.Get(m.snmpOids[i:end])
		if err != nil {
			log.Printf("selected OIDS %+v", m.snmpOids[i:end])
			errLog("SNMP (%s) get error: %s\n", snmp.Target, err)
			errs++
		}
		sent++
		for _, pdu := range pkt.Variables {
			log.Printf("DEBUG pdu:%+v", pdu)
			if pdu.Value == nil {
				continue
			}
			oid := pdu.Name
			val := pdu.Value
			if metric, ok := m.oidSnmpMap[oid]; ok {
				log.Println("OK measurement ", m.cfg.id, "SNMP RESULT OID", oid, "MetricFound", val)
				metric.setRawData(pduVal2Int64(pdu), now)
			} else {
				log.Println("ERROR OID", oid, "Not Found in measurement", m.cfg.id)
			}
		}
	}

	return sent, errs, nil
}


func removeDuplicatesUnordered(elements []string) []string {
	encountered := map[string]bool{}

	// Create a map of all unique elements.
	for v := range elements {
		encountered[elements[v]] = true
	}

	// Place all keys from the map into a slice.
	result := []string{}
	for key, _ := range encountered {
		result = append(result, key)
	}
	return result
}

func (m *InfluxMeasurement) loadIndexedLabels(c *SnmpDeviceCfg) error {
	client := c.snmpClient
	log.Println("Looking up column names for:", c.Host, "NAMES", m.cfg.IndexOID)
	pdus, err := client.BulkWalkAll(m.cfg.IndexOID)
	if err != nil {
		fatal("SNMP bulkwalk error", err)
	}
	m.AllIndexedLabels = make(map[string]string)
	for _, pdu := range pdus {
		switch pdu.Type {
		case gosnmp.OctetString:
			i := strings.LastIndex(pdu.Name, ".")
			suffix := pdu.Name[i+1:]
			name := string(pdu.Value.([]byte))
			m.AllIndexedLabels[suffix] = name
		default:
			log.Println("Error in IndexedLabel for host:", c.Host, "IndexLabel:", m.cfg.IndexOID, "ERR: Not String")
		}
	}
	return nil
}

/*
 filterIndexedLabels construct the final index array from all index and filters
*/
func (m *InfluxMeasurement) filterIndexedLabels(f_mode string) error {
	m.CurIndexedLabels = make(map[string]string, len(m.Filterlabels))

	switch f_mode {
	case "file":
		//file filter should compare with all indexed labels with the value (name)
		for k_f, v_f := range m.Filterlabels {
			for k_l, v_l := range m.AllIndexedLabels {
				if k_f == v_l {
					if len(v_f) > 0 {
						// map[k_l]v_f (alias to key of the label
						m.CurIndexedLabels[k_l] = v_f
					} else {
						//map[k_l]v_l (original name)
						m.CurIndexedLabels[k_l] = v_l
					}

				}
			}
		}

	case "OIDCondition":
		for k_f, _ := range m.Filterlabels {
			for k_l, v_l := range m.AllIndexedLabels {
				if k_f == k_l {
					m.CurIndexedLabels[k_l] = v_l
				}
			}
		}

		//confition filter should comapre with all indexed label with the key (number)
	}

	//could be posible to a delete of the non needed arrays  m.AllIndexedLabels //m.Filterlabels
	return nil
}

func (m *InfluxMeasurement) IndexedLabels() error {
	m.CurIndexedLabels = m.AllIndexedLabels
	return nil
}

func (m *InfluxMeasurement) applyOIDCondFilter(c *SnmpDeviceCfg, oidCond string, typeCond string, valueCond string) error {
	client := c.snmpClient
	log.Println("Looking up column names for Condition in:", c.Host, "NAMES", oidCond)
	pdus, err := client.BulkWalkAll(oidCond)
	if err != nil {
		fatal("SNMP bulkwalk error", err)
	}
	m.Filterlabels = make(map[string]string)
	vc, err := strconv.Atoi(valueCond)
	if err != nil {
		return errors.New("only accepted numeric value as value condition current :" + valueCond)
	}
	var vci int64 = int64(vc)
	for _, pdu := range pdus {
		value := pduVal2Int64(pdu)
		var cond bool
		switch typeCond {
		case "eq":
			cond = (value == vci)
		case "lt":
			cond = (value < vci)
		case "gt":
			cond = (value > vci)
		case "ge":
			cond = (value >= vci)
		case "le":
			cond = (value <= vci)
		default:
			log.Println("Error in Condition filter for host:", c.Host, "OidCondition:", oidCond, "Type", typeCond, "value cond:", valueCond)
		}
		if cond == true {
			i := strings.LastIndex(pdu.Name, ".")
			suffix := pdu.Name[i+1:]
			m.Filterlabels[suffix] = ""
		}

	}
	return nil

	log.Println("OID Condition filters not available yet")
	return nil
}

func (m *InfluxMeasurement) applyFileFilter(file string, enableAlias bool) error {
	log.Println("apply File filter :", file, "Enable Alias:", enableAlias)
	if len(file) == 0 {
		return errors.New("File error ")
	}
	data, err := ioutil.ReadFile(filepath.Join(appdir, file))
	if err != nil {
		log.Fatal(err)
	}
	m.Filterlabels = make(map[string]string)
	for l_num, line := range strings.Split(string(data), "\n") {
		//		log.Println("LINIA:", line)
		// strip comments
		comment := strings.Index(line, "#")
		if comment >= 0 {
			line = line[:comment]
		}
		if len(line) == 0 {
			continue
		}
		f := strings.Fields(line)
		switch len(f) {
		case 1:
			m.Filterlabels[f[0]] = ""

		case 2:
			if enableAlias {
				m.Filterlabels[f[0]] = f[1]
			} else {
				m.Filterlabels[f[0]] = ""
			}

		default:
			log.Println("Error in numero de parametros de fichero: ", file, "Lnum: ", l_num, "num :", len(f), "line:", line)
		}
	}
	return nil
}
