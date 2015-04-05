package io.pivotal.pxf.plugins.gemfire;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.ColumnDescriptor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

public class GemfireResolver extends Plugin implements ReadResolver {

	private static final Log LOG = LogFactory.getLog(GemfireResolver.class);

	private ArrayList<OneField> fields = new ArrayList<OneField>();

	public GemfireResolver(InputData input) {
		super(input);
	}

	@Override
	public List<OneField> getFields(OneRow paramOneRow) throws Exception {
		fields.clear();

		Struct struct = (Struct) paramOneRow.getData();
LOG.info("Columns Count:" + this.inputData.getColumns());
		for (int i = 0; i < this.inputData.getColumns(); i++) {
			ColumnDescriptor cd = this.inputData.getColumn(i);
			DataType columnType = DataType.get(cd.columnTypeCode());
			LOG.info("STRUCT: " + struct);
			Object columnValue = (!isRegionSet()) ? struct.get(cd.columnName()) : ((PdxInstance) struct.get("value"))
			        .getField(cd.columnName());
			LOG.info("VALUE: " + columnValue);
			OneField field = new OneField(columnType.getOID(), toTypedValue(columnType, columnValue));
			fields.add(field);
		}

		return fields;
	}

	private Object toTypedValue(DataType type, Object value) throws IOException {

		if (value == null) {
			return value;
		}

		switch (type) {
		case BIGINT:
			return Long.valueOf(value.toString());
		case BOOLEAN:
			return Boolean.valueOf(value.toString());
		case BPCHAR:
		case CHAR:
			return value.toString().charAt(0);
		case BYTEA:
			return value.toString().getBytes();
		case FLOAT8:
		case REAL:
			return Double.valueOf(value.toString());
		case INTEGER:
		case SMALLINT:
			return Integer.valueOf(value.toString());
		case TEXT:
		case VARCHAR:
			return value.toString();
		default:
			throw new IOException("Unsupported type " + type);

		}
	}

	private boolean isRegionSet() {
		return !StringUtils.isBlank(inputData.getUserProperty(Parameters.OQL_REGION_PARAM));
	}
}
