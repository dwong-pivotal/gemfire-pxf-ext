package io.pivotal.pxf.plugins.gemfire;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.pivotal.pxf.PxfUnit;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.io.DataType;

public class GemfireExtensionTest extends PxfUnit {

	private static final String GEMFIRE_URL = "ambari:10334";
	private static ArrayList<Pair<String, DataType>> columnDefs;
	private static List<Pair<String, String>> extraParams = new ArrayList<Pair<String, String>>();

	static {
		columnDefs = new ArrayList<Pair<String, DataType>>();
		columnDefs.add(new Pair<String, DataType>("companyId", DataType.INTEGER));
		columnDefs.add(new Pair<String, DataType>("firstName", DataType.TEXT));
	}

	@Test
	public void testRegion() throws IllegalArgumentException, Exception {
		extraParams.add(new Pair<String, String>("LOCATORS", GEMFIRE_URL));
		extraParams.add(new Pair<String, String>("REGION", "regionEmployee"));

		List<String> output = new ArrayList<String>();

		output.add("101,Christian");
		output.add("102,Bosa");

		super.assertUnorderedOutput(new Path("/gemfire"), output);
	}

	@Test
	public void testOQLQuery() throws IllegalArgumentException, Exception {
		extraParams.add(new Pair<String, String>("LOCATORS", GEMFIRE_URL));
		extraParams.add(new Pair<String, String>("QUERY",
		        "select e.key, e.value.companyId, e.value.firstName from /regionEmployee.entrySet e"));

		List<String> output = new ArrayList<String>();

		output.add("101,Christian");
		output.add("102,Bosa");

		super.assertUnorderedOutput(new Path("/gemfire"), output);
	}

	@Override
	public List<Pair<String, String>> getExtraParams() {
		return extraParams;
	}

	@Override
	public Class<? extends Fragmenter> getFragmenterClass() {
		return GemfireFragmenter.class;
	}

	@Override
	public Class<? extends ReadAccessor> getReadAccessorClass() {
		return GemfireAccessor.class;
	}

	@Override
	public Class<? extends ReadResolver> getReadResolverClass() {
		return GemfireResolver.class;
	}

	@Override
	public List<Pair<String, DataType>> getColumnDefinitions() {
		return columnDefs;
	}
}
