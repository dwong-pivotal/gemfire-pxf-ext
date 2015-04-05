package io.pivotal.pxf.plugins.gemfire.experiment;

import java.util.Iterator;

import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.pdx.PdxInstance;

public class SimpleClient2 {

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {

		// String oqlQuery = "select e.key, e.value, e.value.email from /regionEmployee.entrySet e";
		String oqlQuery = "select e.key, e.value, e.value.companyId, e.value.email from /regionEmployee.entrySet e";
		String locatorHost = "ambari";
		int locatorPort = 10334;

		/*		ClientCache clientCache = new ClientCacheFactory().set("log-file", "client-cache.log")
		        .set("statistic-archive-file", "gf-client.gfs").set("statistic-sampling-enabled", "true")
		        .set("cache-xml-file", "").addPoolLocator(locatorHost, locatorPort).create(); */
		
		ClientCache clientCache = new ClientCacheFactory().addPoolLocator(locatorHost, locatorPort).create();

		QueryService queryService = clientCache.getQueryService();

		SelectResults<Struct> results = (SelectResults<Struct>) queryService.newQuery(oqlQuery).execute();

		// System.out.println(results.size());

		Iterator<Struct> iterator = results.iterator();
		while (iterator.hasNext()) {
			Struct struc = iterator.next();
			System.out.println(">>>> :" + struc.get("key") + " : "
			        + ((PdxInstance) struc.get("value")).getField("firstname"));
			System.out.println(">>>> :" + struc.get("key") + " : " + struc.get("companyId") + " : "
			        + struc.get("value"));
			// System.out.println(struc.getFieldValues().length);
			// System.out.println(">>>> :" + struc.get("key") + " : " + ((PdxInstance)
			// struc.getFieldValues()[1]).getFieldNames());
		}
		queryService.closeCqs();
		clientCache.close();
		System.exit(0);

	}

}
