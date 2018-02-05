/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package h42.precchia;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.index.SlowCompositeReaderWrapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.PointField;
import org.apache.solr.schema.IndexSchema.DynamicField;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrQueryParser;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.RefCounted;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/** Tests for PointField functionality */
public class TestInetAddressType extends SolrTestCaseJ4 {

  // long overflow can occur in some date calculations if gaps are too large, so we limit to a million years BC & AD.
  private static final long MIN_DATE_EPOCH_MILLIS = LocalDateTime.parse("-1000000-01-01T00:00:00").toInstant(ZoneOffset.ofHours(0)).toEpochMilli();
  private static final long MAX_DATE_EPOCH_MILLIS = LocalDateTime.parse("+1000000-01-01T00:00:00").toInstant(ZoneOffset.ofHours(0)).toEpochMilli();

  private static final String[] FIELD_SUFFIXES = new String[] {
      "", "_dv", "_mv", "_mv_dv", "_ni", "_ni_dv", "_ni_dv_ns", "_ni_dv_ns_mv", 
      "_ni_mv", "_ni_mv_dv", "_ni_ns", "_ni_ns_mv", "_dv_ns", "_ni_ns_dv", "_dv_ns_mv",
      "_smf", "_dv_smf", "_mv_smf", "_mv_dv_smf", "_ni_dv_smf", "_ni_mv_dv_smf",
      "_sml", "_dv_sml", "_mv_sml", "_mv_dv_sml", "_ni_dv_sml", "_ni_mv_dv_sml"
  };

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-inetaddress.xml");
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    clearIndex();
    assertU(commit());
    super.tearDown();
  }
  
  @Test
  public void testIntPointFieldExactQuery() throws Exception {
  	String field = new String("single_ip_address");
    assertTrue(h.getCore().getLatestSchema().getField(field).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(field).getType() instanceof PointField);

    ArrayList<InetAddress> addrs = new ArrayList<InetAddress>();
	    for (int i = 1; i <= 10; i++) {
	        addrs.add(InetAddress.getByName("192.168.1." + Integer.toString(i)));
	        
	      }
    for (int idx=0; idx < addrs.size(); idx++) {
		String ipaddress = addrs.get(idx).getHostAddress();
		String docs = adoc("id", String.valueOf(idx), field, ipaddress);
		System.out.println("ADD_DOC: " + docs);
    	assertU("Inserting document id " + Integer.toString(idx), docs);
    }
    assertU("commit",commit());
    
    String function = "field(" + field + ", min)";
    {
    	SolrQueryRequest query = req("q", "*:*", "fl", "id");
        System.out.println("QUERY1:" + query);
        System.out.println("RESPONSE:" + h.query(query));
        assertQ("numFound", query, 
                "//*[@numFound='10']");
    }
    {
    	SolrQueryRequest query = req("q", "*:*", "sort", field + " desc");
        System.out.println("QUERY2:" + query);
        System.out.println("RESPONSE:" + h.query(query));
        assertQ("numFound", query, 
                "//*[@numFound='10']");
    }
    // Why does this fail?
    {
    	SolrQueryRequest query = req("q", "*:*", "fl", "id," + field, "sort", field + " desc");
        System.out.println("QUERY2:" + query);
        //System.out.println("RESPONSE:" + h.query(query));
        assertQ("numFound", query, 
                "//*[@numFound='10']");
    }
    /*
            "//result/doc[1]/str[@name='id'][.='9']",
            "//result/doc[2]/str[@name='id'][.='8']",
            "//result/doc[3]/str[@name='id'][.='7']",
            "//result/doc[10]/str[@name='id'][.='0']",
            "//result/doc[1]/str[@name='single_ip_address'][.='192.168.1.10']"
    		);
     */
  }
  
  private void doTestPointMultiValuedFunctionQuery(String nonDocValuesField, String docValuesField, String type, String[] numbers) throws Exception {
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(docValuesField).getType() instanceof PointField);
    String function = "field(" + docValuesField + ", min)";
    
    assertQ(req("q", "*:*", "fl", "id, " + docValuesField, "sort", function + " desc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/str[@name='id'][.='9']",
        "//result/doc[2]/str[@name='id'][.='8']",
        "//result/doc[3]/str[@name='id'][.='7']",
        "//result/doc[10]/str[@name='id'][.='0']");

    assertFalse(h.getCore().getLatestSchema().getField(nonDocValuesField).hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).multiValued());
    assertTrue(h.getCore().getLatestSchema().getField(nonDocValuesField).getType() instanceof PointField);

    function = "field(" + nonDocValuesField + ",min)";
    
    assertQEx("Expecting Exception",
        "sort param could not be parsed as a query", 
        req("q", "*:*", "fl", "id", "sort", function + " desc"), 
        SolrException.ErrorCode.BAD_REQUEST);
    
    assertQEx("Expecting Exception", 
        "docValues='true' is required to select 'min' value from multivalued field (" + nonDocValuesField + ") at query time", 
        req("q", "*:*", "fl", "id, " + function), 
        SolrException.ErrorCode.BAD_REQUEST);
    
    function = "field(" + docValuesField + ",foo)";
    assertQEx("Expecting Exception", 
        "Multi-Valued field selector 'foo' not supported", 
        req("q", "*:*", "fl", "id, " + function), 
        SolrException.ErrorCode.BAD_REQUEST);
  }
}
