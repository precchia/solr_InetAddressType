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
package eu.horizon42;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

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
import org.apache.solr.schema.IndexSchema;
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

	String field_str = new String("ip_address_str");
	String field_bin = new String("ip_address_bin");

	@SuppressWarnings("unchecked")
	@BeforeClass
	public static void beforeClass() throws Exception {
		// This testing approach means no schema file or per-test temp solr-home!
		System.setProperty("managed.schema.mutable", "true");
		System.setProperty("managed.schema.resourceName", "schema-inetaddress.xml");
		//System.setProperty("enable.update.log", "false");
		//System.setProperty("documentCache.enabled", "true");
		//System.setProperty("enableLazyFieldLoading", "true");

		initCore("solrconfig-managed-schema.xml", "ignoredSchemaName");

		// TODO SOLR-10229 will make this easier
		boolean PERSIST_FALSE = false; // don't write to test resource dir
		IndexSchema schema = h.getCore().getLatestSchema();
		schema = schema.addFieldTypes(
				Arrays.asList(
						schema.newFieldType("ip_address_str", "eu.horizon42.InetAddressType",map(
								"name", "ip_address_str",
								"class","eu.horizon42.InetAddressType",
								"docValues", "true",
								"indexed", "true",
								"stored", "true",
								"multiValued", "false"
								// "storedDocValue", "string" // string by default
								)),
						schema.newFieldType("ip_address_bin", "eu.horizon42.InetAddressType",map(
								"name", "ip_address_bin",
								"class","eu.horizon42.InetAddressType",
								"docValues", "true",
								"indexed", "true",
								"stored", "true",
								"multiValued", "false",
								"storedDocValue", "binary"
								))
						),PERSIST_FALSE);

		schema = schema.addFields(
				Arrays.asList(
						schema.newField("ip_address_str", "ip_address_str", map()),
						schema.newField("ip_address_bin", "ip_address_bin", map())
						),
				Collections.emptyMap(),
				PERSIST_FALSE);

		h.getCore().setLatestSchema(schema);
		//initCore("solrconfig.xml","schema-inetaddress.xml");
	}

	@Override
	@After
	public void tearDown() throws Exception {
		clearIndex();
		assertU(commit());
		super.tearDown();
	}

	private void addDocuments() {
		ArrayList<InetAddress> addrs = new ArrayList<InetAddress>();
		for (int i = 1; i <= 10; i++) {
			String ipAddress = "192.168.1." + Integer.toString(i);
			try {
				addrs.add(InetAddress.getByName(ipAddress));
			}
			catch (java.net.UnknownHostException ex) {
				fail("Got and Unknwn host exception when inserting : " + ipAddress);
			}
		}
		for (int idx=0; idx < addrs.size(); idx++) {
			String ipaddress = addrs.get(idx).getHostAddress();
			String docs = adoc("id", String.valueOf(idx), field_str, ipaddress, field_bin, ipaddress);
			assertU("Inserting document id " + Integer.toString(idx), docs);
		}
		assertU("commit",commit());
	}

	@Test
	public void testFieldType() throws Exception {
		assertTrue(h.getCore().getLatestSchema().getField(field_str).hasDocValues());
		assertTrue(h.getCore().getLatestSchema().getField(field_str).getType() instanceof PointField);
		assertTrue(h.getCore().getLatestSchema().getField(field_bin).hasDocValues());
		assertTrue(h.getCore().getLatestSchema().getField(field_bin).getType() instanceof PointField);
	}

	@Test
	public void testInetAddressExactQuery() throws Exception {
		addDocuments();

		String function = "field(" + field_str + ",min)";
		// We cannot render (fl=) a field with binary docValues. use string docValues instead
		try {
			SolrQueryRequest query = req("q", "*:*", "fl", "id," + field_str);
			assertQ("numFound", query, 
					"//*[@numFound='10']");
		}
		catch (AssertionError ex) {
			System.out.println("got a AssertionError somewhere:" + ex);
			ex.printStackTrace();
			throw(ex);
		}
	}

	@Test
	public void testSortBinaryValues() throws Exception {
		addDocuments();
		// If we sort using binary docValues, order will be correct: 192.168.1.10 comes last
		{
			SolrQueryRequest query = req("q", "*:*", "fl", "id," + field_str, "sort", field_bin + " asc");
			assertQ("numFound", query, 
					"//*[@numFound='10']",
					"//result/doc[1]/str[@name='ip_address_str'][.='192.168.1.1']",
					"//result/doc[2]/str[@name='ip_address_str'][.='192.168.1.2']",
					"//result/doc[10]/str[@name='ip_address_str'][.='192.168.1.10']"
					);
		}
	}

	@Test
	public void testSortStringValues() throws Exception {
		addDocuments();
		// If we sort using string docValues, order will be incorrect: 192.168.1.10 comes before 192.168.1.2
		{
			SolrQueryRequest query = req("q", "*:*", "fl", "id," + field_str, "sort", field_str + " asc");
			assertQ("numFound", query,
					"//*[@numFound='10']",
					"//result/doc[1]/str[@name='ip_address_str'][.='192.168.1.1']",
					"//result/doc[2]/str[@name='ip_address_str'][.='192.168.1.10']",
					"//result/doc[3]/str[@name='ip_address_str'][.='192.168.1.2']"
					);
		}
	}

	@Test
	public void testRetrieveBinaryDocValue() throws Exception {
		addDocuments();
		// This should crash - because we ask to render a binary docValue and ByteArray.toString will be unhappy
		{
			try {
				SolrQueryRequest query = req("q", "*:*", "fl", "id," + field_bin, "sort", field_bin + " asc");
				fail("Retrieving binary docValues shouldn't work because ByteArray.toString will be unhappy");
			}
			catch (AssertionError e) {
			}	
		}

	}
}
