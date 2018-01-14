# Info


Lucene has an implementation of InetAddressPoint field type (within its misc section, within lucene-misc library)
This is a POC for a InetAddressType implementation in solr which would make use of this

It is not intended in any means to be a complete implementation (although it seems to work pretty wel)

What is missing, so far:
- performance and stress testing
- automated unit and integration tests
- To be tested with SolrCloud (although it should work)

But, other than that, it does quite a good job.
A field can be indexed, stored or generate docValues (i.e. stored="true" indexed="true" docValues="true")

- You can sort by IP Address: sort=src_address asc)
- You can do range queries: q=src_address:\[192.168.1.1 TO 192.168.1.255\]
- You can do set based faceting: 

```
curl http://localhost:8983/solr/ipaddress/select -d 'q=*:*' -d 'sort=src_address asc' -d'facet=true' -d 'facet.interval=src_address' -d 'facet.interval.set=[192.168.1.0,192.168.1.255]' -d 'facet.interval.set=[192.168.2.0,192.168.2.255]' -d 'rows=0'
```


There is no way to do range-based faceting. That is due to how solr and range faceting is currently implemented, 

This field Type has been tested on solr 7.1.0
It should work on solr 6.6.0. But so far this has not been tested;

# Usage

## InetAddressType field type

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

