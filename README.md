influxsnmp
==========
Poll network devices via SNMP and save the data in InfluxDB (version 0.1X.X)

Based on the original work from Paul Stuart (https://github.com/paulstuart/influxsnmp)

These are the main differences with its predecessor.

* Changed he way to configure metrics , no longer needed the "pre-digested" file oids.txt , in its place we need to define each metric and how we want influxsnmp will compute data and sent as measurements.
* Influxsnmp now supports any kind of snmp data (direct values) or (indexed values) which can be filtered and also "Aliased" to customize the name we would like to see in our influxdb.
* Added SNMP v3 support.
* changed config file format to TOML (a little more flexible than its precedesor ini based file)
