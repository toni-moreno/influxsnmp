package main

import (
	ers "errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/soniah/gosnmp"
)

type Control struct {
	nameOid string
	found   map[string]string
	labels  map[string]string
	usable  map[string]struct{}
}

var (
	errorSNMP int
	nameOid   = "1.3.6.1.2.1.31.1.1.1.1" // ifName
)

const (
	maxOids = 60 // const in gosnmp
)

type pduValue struct {
	name, column string
	value        interface{}
}

func getPoint(cfg *SnmpConfig, pdu gosnmp.SnmpPDU) *pduValue {
	i := strings.LastIndex(pdu.Name, ".")
	root := pdu.Name[1:i]
	suffix := pdu.Name[i+1:]
	col := cfg.labels[cfg.asOID[suffix]]
	name, ok := oidToName[root]
	if verbose {
		log.Println("ROOT:", root, "SUFFIX:", suffix, "COL:", col, "NAME:", "VALUE:", pdu.Value)
	}
	if !ok {
		log.Printf("Invalid oid: %s\n", pdu.Name)
		return nil
	}
	if len(col) == 0 {
		log.Println("empty col for:", cfg.asOID[suffix])
		return nil // not an OID of interest
	}
	return &pduValue{name, col, pdu.Value}
}

func bulkPoint(cfg *SnmpConfig, pdu gosnmp.SnmpPDU) *pduValue {
	i := strings.LastIndex(pdu.Name, ".")
	root := pdu.Name[1:i]
	suffix := pdu.Name[i+1:]
	col := cfg.asOID[suffix]
	name, ok := oidToName[root]
	if verbose {
		log.Println("ROOT:", root, "SUFFIX:", suffix, "COL:", col, "NAME:", "VALUE:", pdu.Value)
	}
	if !ok {
		log.Printf("Invalid oid: %s\n", pdu.Name)
		return nil
	}
	if len(col) == 0 {
		log.Println("empty col for:", suffix)
		return nil // not an OID of interest
	}
	return &pduValue{name, col, pdu.Value}
}

func snmpStats(snmp *gosnmp.GoSNMP, cfg *SnmpConfig) error {
	now := time.Now()
	if cfg == nil {
		log.Fatal("cfg is nil")
	}
	if cfg.Influx == nil {
		log.Fatal("influx cfg is nil")
	}
	bps := cfg.Influx.BP()
	// we can only get 'maxOids' worth of snmp requests at a time
	for i := 0; i < len(cfg.oids); i += maxOids {
		end := i + maxOids
		if end > len(cfg.oids) {
			end = len(cfg.oids)
		}
		cfg.incRequests()
		pkt, err := snmp.Get(cfg.oids[i:end])
		if err != nil {
			errLog("SNMP (%s) get error: %s\n", cfg.Host, err)
			cfg.incErrors()
			cfg.LastError = now
			return err
		}
		cfg.incGets()
		if verbose {
			log.Println("SNMP GET CNT:", len(pkt.Variables))
		}
		for _, pdu := range pkt.Variables {
			val := getPoint(cfg, pdu)
			if val == nil {
				continue
			}
			if val.value == nil {
				continue
			}
			pt := makePoint(cfg.Host, cfg.TagMap, val, now)
			//bps.Points = append(bps.Points, pt)
			(*bps).AddPoint(pt)

		}
	}
	cfg.Influx.Send(bps)
	return nil
}

func bulkStats(snmp *gosnmp.GoSNMP, cfg *SnmpConfig) error {
	now := time.Now()
	if cfg == nil {
		log.Fatal("cfg is nil")
	}
	if cfg.Influx == nil {
		log.Fatal("influx cfg is nil")
	}
	bps := cfg.Influx.BP()
	addPacket := func(pdu gosnmp.SnmpPDU) error {
		val := bulkPoint(cfg, pdu)
		if val != nil && val.value != nil {
			pt := makePoint(cfg.Host, cfg.TagMap, val, now)
			//bps.Points = append(bps.Points, pt)
			(*bps).AddPoint(pt)

		}
		return nil
	}
	for i := 0; i < len(cfg.oids); i += 1 {
		cfg.incRequests()
		if err := snmp.BulkWalk(cfg.oids[i], addPacket); err != nil {
			errLog("SNMP (%s) get error: %s\n", cfg.Host, err)
			cfg.incErrors()
			cfg.LastError = now
			return err
		}
	}
	cfg.Influx.Send(bps)
	return nil
}

func printSnmpNames(c *SnmpConfig) {
	client, err := snmpClient(c)
	if err != nil {
		fatal(err)
	}
	defer client.Conn.Close()
	pdus, err := client.BulkWalkAll(nameOid)
	//pdus, err := client.WalkAll(nameOid)
	if err != nil {
		fatal("SNMP bulkwalk error", err)
	}
	for _, pdu := range pdus {
		switch pdu.Type {
		case gosnmp.OctetString:
			fmt.Println(string(pdu.Value.([]byte)), pdu.Name)
		}
	}
}

func snmpClient(s *SnmpConfig) (*gosnmp.GoSNMP, error) {
	var client *gosnmp.GoSNMP
	hostIPs, _ := net.LookupHost(s.Host)
	if len(hostIPs) > 1 {
		log.Println("Lookup for %s host has more than one IP: %v", s.Host, hostIPs)
	}
	switch s.SnmpVersion {
	case "1":
		client = &gosnmp.GoSNMP{
			Target:  hostIPs[0],
			Port:    uint16(s.Port),
			Version: gosnmp.Version1,
			Timeout: time.Duration(s.Timeout) * time.Second,
			Retries: s.Retries,
		}
	case "2c":
		//validate community
		if len(s.Community) < 1 {
			log.Printf("Error no community found %s in host %s", s.Community, s.Host)
			return nil, ers.New("Error on snmp community")
		}

		client = &gosnmp.GoSNMP{
			Target:    hostIPs[0],
			Port:      uint16(s.Port),
			Community: s.Community,
			Version:   gosnmp.Version2c,
			Timeout:   time.Duration(s.Timeout) * time.Second,
			Retries:   s.Retries,
		}
	case "3":

		authpmap := map[string]gosnmp.SnmpV3AuthProtocol{
			"NoAuth": gosnmp.NoAuth,
			"MD5":    gosnmp.MD5,
			"SHA":    gosnmp.SHA,
		}
		privpmap := map[string]gosnmp.SnmpV3PrivProtocol{
			"NoPriv": gosnmp.NoPriv,
			"DES":    gosnmp.DES,
			"AES":    gosnmp.AES,
		}
		UsmParams := new(gosnmp.UsmSecurityParameters)

		if len(s.V3AuthUser) < 1 {
			log.Printf("Error username not found in snmpv3 %s in host %s", s.V3AuthUser, s.Host)
			//			log.Printf("DEBUG SNMP: %+v", *s)
			return nil, ers.New("Error on snmp v3 user")
		}

		switch s.V3SecLevel {

		case "NoAuthNoPriv":
			log.Println("Selected NO Auth - No Priv")
			UsmParams = &gosnmp.UsmSecurityParameters{
				UserName:               s.V3AuthUser,
				AuthenticationProtocol: gosnmp.NoAuth,
				//	AuthenticationPassphrase: "",
				PrivacyProtocol: gosnmp.NoPriv,
				//	PrivacyPassphrase:        "",
			}
		case "AuthNoPriv":
			log.Println("Selected SI Auth - No Priv")
			if len(s.V3AuthPass) < 1 {
				log.Printf("Error password not found in snmpv3 %s in host %s", s.V3AuthUser, s.Host)
				//			log.Printf("DEBUG SNMP: %+v", *s)
				return nil, ers.New("Error on snmp v3 AuthPass")
			}

			//validate correct s.authuser

			if val, ok := authpmap[s.V3AuthProt]; !ok {
				log.Printf("Error in Auth Protocol %s | %s  in host %s", s.V3AuthProt, val, s.Host)
				return nil, ers.New("Error on snmp v3 AuthProt")
			}

			//validate s.authpass s.authprot
			UsmParams = &gosnmp.UsmSecurityParameters{
				UserName:                 s.V3AuthUser,
				AuthenticationProtocol:   authpmap[s.V3AuthProt],
				AuthenticationPassphrase: s.V3AuthPass,
				PrivacyProtocol:          gosnmp.NoPriv,
				//	PrivacyPassphrase:        "",
			}
		case "AuthPriv":
			log.Println("Selected SI Auth - Priv")
			//validate s.authpass s.authprot

			if len(s.V3AuthPass) < 1 {
				log.Printf("Error password not found in snmpv3 %s in host %s", s.V3AuthUser, s.Host)
				//				log.Printf("DEBUG SNMP: %+v", *s)
				return nil, ers.New("Error on snmp v3 AuthPass")
			}

			if val, ok := authpmap[s.V3AuthProt]; !ok {
				log.Printf("Error in Auth Protocol %s | %s  in host %s", s.V3AuthProt, val, s.Host)
				return nil, ers.New("Error on snmp v3 AuthProt")
			}

			//validate s.privpass s.privprot

			if len(s.V3PrivPass) < 1 {
				log.Printf("Error privPass not found in snmpv3 %s in host %s", s.V3AuthUser, s.Host)
				//		log.Printf("DEBUG SNMP: %+v", *s)
				return nil, ers.New("Error on snmp v3 PrivPAss")
			}

			if val, ok := privpmap[s.V3PrivProt]; !ok {
				log.Printf("Error in Priv Protocol %s | %s  in host %s", s.V3PrivProt, val, s.Host)
				return nil, ers.New("Error on snmp v3 AuthPass")
			}

			UsmParams = &gosnmp.UsmSecurityParameters{
				UserName:                 s.V3AuthUser,
				AuthenticationProtocol:   authpmap[s.V3AuthProt],
				AuthenticationPassphrase: s.V3AuthPass,
				PrivacyProtocol:          privpmap[s.V3PrivProt],
				PrivacyPassphrase:        s.V3PrivPass,
			}
		}
		//		fmt.Printf("DEBUG: USMPARAMS %+v\n", *UsmParams)
		client = &gosnmp.GoSNMP{
			Target:             hostIPs[0],
			Port:               uint16(s.Port),
			Version:            gosnmp.Version3,
			Timeout:            time.Duration(s.Timeout) * time.Second,
			Retries:            s.Retries,
			SecurityModel:      gosnmp.UserSecurityModel,
			MsgFlags:           gosnmp.AuthPriv,
			SecurityParameters: UsmParams,
		}
		//		fmt.Printf("DEBUG: CLIENT %+v\n", *client)
	default:
		log.Printf("Error no snmpversion found %s in host %s", s.SnmpVersion, s.Host)
		return nil, ers.New("Error on snmp Version")
	}
	if s.Debug {
		//client.Logger = log.New(os.Stdout, "", 0)
		client.Logger = s.DebugLog()
	}
	err := client.Connect()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	return client, err
}

func (s *SnmpConfig) DebugLog() *log.Logger {
	name := filepath.Join(logDir, "debug_"+strings.Replace(s.Host, ".", "-", -1)+".log")
	if l, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0664); err == nil {
		return log.New(l, "", 0)
	} else {
		fmt.Fprintln(os.Stderr, err)
		return nil
	}
}

func (s *SnmpConfig) Gather(count int, wg *sync.WaitGroup) {
	debug := false
	client, err := snmpClient(s)
	if err != nil {
		fatal(err)
	}
	defer client.Conn.Close()
	spew(strings.Join(s.oids, "\n"))
	fn := snmpStats
	if len(s.PortFile) == 0 {
		fn = bulkStats
	}
	c := time.Tick(time.Duration(s.Freq) * time.Second)
	for {
		err := fn(client, s)
		if count > 0 {
			count--
			if count == 0 {
				break
			}
		}
		// was seeing clients getting "wedged" -- so just restart
		if err != nil {
			errLog("snmp error - reloading snmp client: %s", err)
			client.Conn.Close()
			for {
				if client, err = snmpClient(s); err == nil {
					break
				}
				errLog("snmp client connect error: %s", err)
				time.Sleep(time.Duration(s.Timeout) * time.Second)
			}
		}

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
