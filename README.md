# Info


Lucene has an implementation of InetAddressPoint field type (within its misc section, within lucene-misc library).
This project was meant as a POC for a InetAddressType implementation in solr which would make use of it.

It is not intended in any means to be a complete implementation (although it seems to work pretty well)

What is missing, so far:
- No unit and integration tests
- Not tested for performance
- To be tested with SolrCloud (although it should work)
- a lot of debugging code.

But, other than that, it does quite a good job.
A field can be indexed, stored or generate docValues (i.e. stored="true" indexed="true" docValues="true")

- You can sort by IP Address: sort=src_address asc
- You can do range queries: q=src\_address:\[192.168.1.1 TO 192.168.1.255\]
- You can do interval based faceting: http://localhost:8983/solr/ipaddress/select?facet=on&q=\*:\*&rows=0&facet.interval=src_address&facet.interval.set=\[192.168.1.1,192.168.1.255\]&facet.interval.set=\[192.168.2.1,192.168.2.255\]

There is no way to do range-based faceting. That is due to how solr and range faceting is currently implemented, 

This field Type has been tested on solr 7.1.0
It should work on previous versions also. But has not been tested yet;

# Usage
### build jar
The project is a standard maven project:

```
mvn clean package
```

The above command should create a file named solr-inetaddress.jar within target directory of the project

### add jar
Add the jar to the classpath
e.g. (solrconfig.xml):

```xml
<config>
<!-- ... -->
<lib path="/path/to/your/solr-inetaddress.jar" />
</config>
```

Or, create a lib directory within your instanceDir and copy the jar file there

The jar depends on lucene-misc-<version>.jar (where the InetAddressPoint class resides). This file should be already present within solr libraries

 
### Add fieldType and fields
Add some fieldTypes of class h42.precchia.InetAddressType to solr.
Add some fields of that type

e.g. (schema.xml):

```xml
<schema name="example" version="1.6">
<!-- ... -->
 <fieldType name="ip_address" class="h42.precchia.InetAddressType" docValues="true" indexed="true" stored="true" />
<!-- ... -->
<field name="dst_address" type="ip_address" indexed="true" stored="true" docValues="true"/>
<!-- ... -->
</schema>
```

Or through API:

```
curl "http://localhost:8983/solr/ipaddress/schema" -H 'Content-type:application/json' -d "$(cat create_fields.json)"
```
 
create_fields.json:

```
"add-field-type":{
  "name":"ip_address",
  "class":"h42.precchia.InetAddressType"
},
"add-field" : {
  "name":"src_address",
  "type":"ip_address",
  "stored":true,
  "indexed":true,
  "docValues":true
},
"add-field" : {
  "name":"dst_address",
  "type":"ip_address",
  "stored":true,
  "indexed":true,
  "docValues":true
}
```
You now have fields src\_address and dest\_address of type ip_address (both IPV4 and IPV6).
