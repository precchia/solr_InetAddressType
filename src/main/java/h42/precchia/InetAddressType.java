package h42.precchia;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.PointField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;

/**
 * InetAddressType PointField
 */

// Why inheriting from PrimitiveType:
// an InetAddress field type is kind of a NumericFieldType. But NumericType can only be integer, float, long, double, date
// PrimitiveFieldType is a thin subclass level above FieldType, which sets omitnorms to true
// PointField is a NumericFieldType but on top will implement toInternalBytesRef instead of toInternal

public class InetAddressType extends PointField {
	private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	/* (non-Javadoc)
	 * @see org.apache.solr.schema.PrimitiveFieldType#init(org.apache.solr.schema.IndexSchema, java.util.Map)
	 */
	// We probably would need to override this to personalize params we accept
	@Override
	protected void init(IndexSchema schema, Map<String, String> args) {
	    log.debug("init:" + schema + "; args:" + args);
		super.init(schema, args);
	}

	/* (non-Javadoc)
	 * @see org.apache.solr.schema.FieldType#createFields(org.apache.solr.schema.SchemaField, java.lang.Object)
	 * This class needs to be overriden if you want to handle stored AND indexed fields together
	 * createFields is the method that will return the several fields (indexed, docValue, stored) from one entry
	 */
	@Override
	public List<IndexableField> createFields(SchemaField sf, Object value) {
		log.trace("createFields:" + sf + " : " + value);

	    // If not used at all, return an epmty list
		if (!isFieldUsed(sf)) {
	        return Collections.emptyList();
	    }
		List<IndexableField> fields = new ArrayList<>(3);
		IndexableField field = null;
		
		// Doing this way we call twice toNativeType. See how this impacts performance
		InetAddress nativeValue = (InetAddress) toNativeType(value);
		 
		// stored?
		if (sf.stored()) {
			fields.add(getStoredField(sf, value));
		}

		// indexed?
		if (sf.indexed()) {
			fields.add(createField(sf, value));
		}
	      
		// docValues?
		if (sf.hasDocValues()) {
			fields.add(getDocValuesField(sf,nativeValue));
		} 
		return fields;
	}
	/*
	 * NOTE: If we have both stored and docValues, RetrieveFieldsOptimizer class
	 * (@see org.apache.solr.response.RetrieveFieldsOptimizer)
	 * will use docValues as stored values, as an optimization.
	 * Sequence for rendering a docValues is:
	 * ResponseWriter ->
	 *   RetrieveFieldsOptimizer
	 *   DocsStreamer ->
	 *     SolrDocumentFetcher
	 *     
	 * Within SolrDocumentFetcher the method decorateDocValueFields is responsible for rendering a docValues field.
	 * Unfortunately, the only field which has a rendering managd by the FieldType is SORTED_SET.
	 * All the others are either converted withine SolrDocumentFetcher or returned as is (which is a BytesRef)
	 * Because of this, we need to return a SORTED_SET
	 * 
	 * There must be a cleverer way to do it.
	 */
	
	private Field getDocValuesField(SchemaField sf, InetAddress nativeValue) {
		BytesRef bf = new BytesRef(InetAddressPoint.encode(nativeValue));
        if (!sf.multiValued()) {
	        return new SortedSetDocValuesField(sf.getName(), bf);
        } else {
        	return new SortedDocValuesField(sf.getName(), bf);
        }
	}
	/* (non-Javadoc)
	 * storedField is an intrnal method called by createFields for creating the indexed field 
	 */
	// How do we create a storedField? If value is a string, we just store the string.
	// If it's a InetAddress how do e convert back?
	protected StoredField getStoredField(SchemaField sf, Object value) {
		log.trace("getStoredValue:" + value);
		return new StoredField(sf.getName(), value.toString());
	}

    /* (non-Javadoc)
	 * @see org.apache.solr.schema.FieldType#createField(org.apache.solr.schema.SchemaField, java.lang.Object, float)
	 * createField is called by createFields to create the corresponding indexe field
	 * We override it to return InetAddressPoint corresponding to the value
	 */
	@Override
	public IndexableField createField(SchemaField sf, Object value) {
		log.trace("createfield for: " + value);
    	// Do we receive only String values, or can it happen that we receive InetAddress type?
		InetAddress val = (InetAddress)toNativeType(value);
		return new InetAddressPoint(sf.getName(), val);
	}

	/* (non-Javadoc)
	 * @see org.apache.solr.schema.FieldType#getUninversionType(org.apache.solr.schema.SchemaField)
	 * This is basically the type we return as docValues Type
	 * Only exception: for non multivalued fields we need to return a SortedSetDocValuesField type.
	 * In order to trigger conversion BytesRef -> String when rendering.
	 * However, it should be a BinaryDocValues.
	 */
	  @Override
	  public Type getUninversionType(SchemaField sf) {
	    if (sf.multiValued()) {
	      return Type.SORTED;
	    } else {
	      return Type.BINARY;
	    }
	  }
	
	@Override
	/*
	 * (non-Javadoc)
	 * @see org.apache.solr.schema.FieldType#write(org.apache.solr.response.TextResponseWriter, java.lang.String, org.apache.lucene.index.IndexableField)
	 * Called to write back value
	 */
	public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
		writer.writeStr(name, f.stringValue(), true);
	}

	// **********************************************************************************
	// Conversion functions

	// **********************************************************************************
	/* (non-Javadoc)
	 * @see org.apache.solr.schema.FieldType#toNativeType(java.lang.Object)
	 * Converts from input type to type native to our fieldType
	 * In our case, converts to InetAddress
	 */
	@Override
	public Object toNativeType(Object val) {
		log.trace("toNativeType:" + val);
    	try {
		    if (val instanceof InetAddress) {
		    	return val;
		    } else {
	    	    return InetAddress.getByName((String) val);
			}
		} catch (UnknownHostException e) {
			log.error("unable to understand the format of input: " + val);
			throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
			          "Invalid Address:'" + val + "'",e);
	    }
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.solr.schema.PointField#indexedToReadable(org.apache.lucene.util.BytesRef)
	 */
	@Override
	protected String indexedToReadable(BytesRef indexedForm) {
		log.debug("indexedToReadable: " + indexedForm);
		try {
			InetAddress inet = InetAddress.getByAddress(indexedForm.bytes);
			return inet.getHostAddress();
		} catch (UnknownHostException e) {
			log.error("Error in converting an internal value into readable",e);
		    return null;
		}
	}
	/*
	 * (non-Javadoc)
	 * @see org.apache.solr.schema.FieldType#readableToIndexed(java.lang.CharSequence, org.apache.lucene.util.BytesRefBuilder)
	 * You will need to override this because the default implementation uses tointernal, which generated an error within a PointField
	 */
	  @Override
	  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
		  result.grow(InetAddressPoint.BYTES);
		  result.setLength(InetAddressPoint.BYTES);
		  InetAddress inet = (InetAddress)toNativeType(val);
		  result.copyBytes(InetAddressPoint.encode(inet),0,InetAddressPoint.BYTES);
	  }

		/* (non-Javadoc)
		 * @see org.apache.solr.schema.FieldType#toExternal(org.apache.lucene.index.IndexableField)
		 * This will be called to render the docValues field. Because we have set it as SORTED_SET.
		 * Call sequence:
		 * org.apache.solr.search.SolrDocumentFetcher.decorateDocValueFields -->
		 *   --> toObject(SchemaField,Object) (within FieldType)
		 *     --> indexedToReadable()
		 *     --> createField() (to create the IndexableField)
		 *       --> toNativeType()
		 *     --> toObject(IndexableField)
		 *       --> toExternal(IndexableField)
		 */
		@Override
		public String toExternal(IndexableField f) {
			return InetAddressPoint.decode(f.binaryValue().bytes).getHostAddress();
		}

	// **********************************************************************************
	// Queries
	// **********************************************************************************
	
	/* (non-Javadoc)
	 * @see org.apache.solr.schema.FieldType#getPrefixQuery(org.apache.solr.search.QParser, org.apache.solr.schema.SchemaField, java.lang.String)
	 */
	@Override
	public Query getPrefixQuery(QParser parser, SchemaField field, String termStr) {
	    log.error("Not implemented yet PrefixQuery for termStr:" + termStr);
		// TODO Auto-generated method stub
	    
		return super.getPrefixQuery(parser, field, termStr);
	}

	/* (non-Javadoc)
	 * @see org.apache.solr.schema.FieldType#getRangeQuery(org.apache.solr.search.QParser, org.apache.solr.schema.SchemaField, java.lang.String, java.lang.String, boolean, boolean)
	 * a Rangequery is a query like: src_address:[192.168.1.1 TO 192.168.1.10]
	 * Use a square bracket [ or ] to include the corresponding boundary
	 * Use a parenthesis to exclude it
	 * If query is like: (192.168.1.1 TO 192.168.1.10) then 192.168.1.1 and 192.168.1.10 will be excluded
	 */
	@Override
	public Query getRangeQuery(QParser parser, SchemaField field, String part1, String part2, boolean minInclusive,
			boolean maxInclusive) {
		InetAddress addr1, addr2;
		try {
			addr1 = InetAddress.getByName(part1);
			if (!minInclusive) 
				addr1 = InetAddressPoint.nextUp(addr1);
		} catch (UnknownHostException e) {
			log.error("InetAddress '" + part1 + "' is not recognized as a valid address",e);
			addr1=InetAddressPoint.MIN_VALUE;
		}
		try {
			addr2 = InetAddress.getByName(part2);
			if (!maxInclusive)
				addr2 = InetAddressPoint.nextDown(addr2);
		} catch (UnknownHostException e) {
			log.error("InetAddress '" + part2 + "' is not recognized as a valid address",e);
			addr2=InetAddressPoint.MAX_VALUE;
		}
		return InetAddressPoint.newRangeQuery(field.getName(), addr1, addr2);
	}

	/* (non-Javadoc)
	 * @see org.apache.solr.schema.FieldType#getFieldQuery(org.apache.solr.search.QParser, org.apache.solr.schema.SchemaField, java.lang.String)
	 * Query for one specific value.
	 * Overridden from PointField implementation.
	 */
	@Override
	public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
		log.info("got a query request for field " + field + " searching for " + externalVal);
		try {
			InetAddress address;
			address = InetAddress.getByName(externalVal);
			return InetAddressPoint.newExactQuery(field.getName(), address);
		} catch (UnknownHostException e) {
			log.error("InetAddress '" + externalVal + "' is not recognized as a valid address");
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.solr.schema.PointField#getExactQuery(org.apache.solr.schema.SchemaField, java.lang.String)
	 * We need to override this method because we inherit from PointField. But since we rewrite completely getFieldQuery
	 * We should never be called
	 * That is a side-effect of inheriting from PointField
	 */
	@Override
	protected Query getExactQuery(SchemaField field, String externalVal) {
		log.error("getExactQuery should never be called?",new Throwable());
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.solr.schema.PointField#getPointRangeQuery(org.apache.solr.search.QParser, org.apache.solr.schema.SchemaField, java.lang.String, java.lang.String, boolean, boolean)
	 * We need to override this but since we completely reqrite getFieldQuery and getRangeQuery,
	 * we sould nevver be called
	 * That is a side-effect of inheriting from PointField
	 */
	@Override
	public Query getPointRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive,
			boolean maxInclusive) {
		log.error("getPointRangeQuery should never be called?",new Throwable());
		return null;
	}

	/*
	 * (non-Javadoc)
	 * @see org.apache.solr.schema.FieldType#getSortField(org.apache.solr.schema.SchemaField, boolean)
	 * REQUIRED
	 * This will be called whenever we sort on this field to retrieve the SortField to use
	 */
	  @Override
	  public SortField getSortField(final SchemaField field, final boolean reverse) {
	    field.checkSortability();
	    return new BinarySortField(field.getName(), reverse);
	  }

	  private static class BinarySortField extends SortField {
	    public BinarySortField(final String field, final boolean reverse) {
	      super(field, new FieldComparatorSource() {
	        @Override
	        public FieldComparator.TermOrdValComparator newComparator
	            (final String fieldname, final int numHits, final int sortPos, final boolean reversed) {
	          return new FieldComparator.TermOrdValComparator(numHits, fieldname);
	        }}, reverse);
	    }
	  }

	// **********************************************************************************
	// Stubs there to see IF they get called at any point and if we need to override them
	// **********************************************************************************
	
	@Override
	protected ValueSource getSingleValueSource(org.apache.lucene.search.SortedNumericSelector.Type choice,
			SchemaField field) {
		log.debug("getSingleValueSource: " + choice + "; " + field);
		// TODO What should we return ?
		return null;
	}

	/* (non-Javadoc)
	 * @see org.apache.solr.schema.FieldType#toObject(org.apache.solr.schema.SchemaField, org.apache.lucene.util.BytesRef)
	 */
	@Override
	public Object toObject(SchemaField sf, BytesRef term) {
		Object obj = super.toObject(sf, term); 
		// TODO Auto-generated method stub
		log.trace("toObject " + sf + "; " + term + " = " + obj, new Throwable());
		return obj;
	}

}
