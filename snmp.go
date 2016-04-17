package main

import (
	ers "errors"
	"fmt"
	//	"log"
	"net"
	"os"
	//"strings"
	"time"

	"github.com/soniah/gosnmp"
)

func pduVal2Int64(pdu gosnmp.SnmpPDU) int64 {
	val := pdu.Value
	switch pdu.Type {
	case gosnmp.Counter32:
		return int64(val.(int32))
	case gosnmp.Integer:
		return int64(val.(int))
	case gosnmp.Gauge32:
		return int64(val.(uint))
	case gosnmp.Counter64:
		return val.(int64)
	case gosnmp.Uinteger32:
		return int64(val.(uint32))
	}
	return 0
}

const (
	maxOids = 60 // const in gosnmp
)

func snmpClient(s *SnmpDeviceCfg) (*gosnmp.GoSNMP, error) {
	var client *gosnmp.GoSNMP
	hostIPs, _ := net.LookupHost(s.Host)
	if len(hostIPs) > 1 {
		s.log.Infof("Lookup for %s host has more than one IP: %v", s.Host, hostIPs)
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
			s.log.Errorf("Error no community found %s in host %s", s.Community, s.Host)
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
		seclpmap := map[string]gosnmp.SnmpV3MsgFlags{
			"NoAuthNoPriv": gosnmp.NoAuthNoPriv,
			"AuthNoPriv":   gosnmp.AuthNoPriv,
			"AuthPriv":     gosnmp.AuthPriv,
		}
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
			s.log.Errorf("Error username not found in snmpv3 %s in host %s", s.V3AuthUser, s.Host)
			return nil, ers.New("Error on snmp v3 user")
		}

		switch s.V3SecLevel {

		case "NoAuthNoPriv":
			UsmParams = &gosnmp.UsmSecurityParameters{
				UserName:               s.V3AuthUser,
				AuthenticationProtocol: gosnmp.NoAuth,
				PrivacyProtocol:        gosnmp.NoPriv,
			}
		case "AuthNoPriv":
			if len(s.V3AuthPass) < 1 {
				s.log.Errorf("Error password not found in snmpv3 %s in host %s", s.V3AuthUser, s.Host)
				return nil, ers.New("Error on snmp v3 AuthPass")
			}

			//validate correct s.authuser

			if val, ok := authpmap[s.V3AuthProt]; !ok {
				s.log.Errorf("Error in Auth Protocol %s | %s  in host %s", s.V3AuthProt, val, s.Host)
				return nil, ers.New("Error on snmp v3 AuthProt")
			}

			//validate s.authpass s.authprot
			UsmParams = &gosnmp.UsmSecurityParameters{
				UserName:                 s.V3AuthUser,
				AuthenticationProtocol:   authpmap[s.V3AuthProt],
				AuthenticationPassphrase: s.V3AuthPass,
				PrivacyProtocol:          gosnmp.NoPriv,
			}
		case "AuthPriv":
			//validate s.authpass s.authprot

			if len(s.V3AuthPass) < 1 {
				s.log.Errorf("Error password not found in snmpv3 %s in host %s", s.V3AuthUser, s.Host)
				return nil, ers.New("Error on snmp v3 AuthPass")
			}

			if val, ok := authpmap[s.V3AuthProt]; !ok {
				s.log.Errorf("Error in Auth Protocol %s | %s  in host %s", s.V3AuthProt, val, s.Host)
				return nil, ers.New("Error on snmp v3 AuthProt")
			}

			//validate s.privpass s.privprot

			if len(s.V3PrivPass) < 1 {
				s.log.Errorf("Error privPass not found in snmpv3 %s in host %s", s.V3AuthUser, s.Host)
				//		log.Printf("DEBUG SNMP: %+v", *s)
				return nil, ers.New("Error on snmp v3 PrivPAss")
			}

			if val, ok := privpmap[s.V3PrivProt]; !ok {
				s.log.Errorf("Error in Priv Protocol %s | %s  in host %s", s.V3PrivProt, val, s.Host)
				return nil, ers.New("Error on snmp v3 AuthPass")
			}

			UsmParams = &gosnmp.UsmSecurityParameters{
				UserName:                 s.V3AuthUser,
				AuthenticationProtocol:   authpmap[s.V3AuthProt],
				AuthenticationPassphrase: s.V3AuthPass,
				PrivacyProtocol:          privpmap[s.V3PrivProt],
				PrivacyPassphrase:        s.V3PrivPass,
			}
		default:
			s.log.Errorf("Error no Security Level found %s in host %s", s.V3SecLevel, s.Host)
			return nil, ers.New("Error on snmp Security Level")

		}
		client = &gosnmp.GoSNMP{
			Target:             hostIPs[0],
			Port:               uint16(s.Port),
			Version:            gosnmp.Version3,
			Timeout:            time.Duration(s.Timeout) * time.Second,
			Retries:            s.Retries,
			SecurityModel:      gosnmp.UserSecurityModel,
			MsgFlags:           seclpmap[s.V3SecLevel],
			SecurityParameters: UsmParams,
		}
	default:
		s.log.Errorf("Error no snmpversion found %s in host %s", s.SnmpVersion, s.Host)
		return nil, ers.New("Error on snmp Version")
	}
	if s.SnmpDebug {
		client.Logger = s.DebugLog()
	}
	err := client.Connect()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	return client, err
}
