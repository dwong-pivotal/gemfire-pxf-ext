package io.pivotal.pxf.plugins.gemfire;

import java.util.Iterator;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadAccessor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;

public class GemfireAccessor extends Plugin implements ReadAccessor {

	private static final Log LOG = LogFactory.getLog(GemfireAccessor.class);

	private static final String GF_KEY = "key";
	private Parameters params;
	private ClientCache clientCache;
	private Iterator<Struct> resultIterator;
	private OneRow oneRow = new OneRow();

	public GemfireAccessor(InputData input) throws MissingArgumentException {
		super(input);
		params = new Parameters(input);
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean openForRead() throws Exception {
		try {
			String locatorHostUrl = params.getLocators()[0];

			LOG.info(Parameters.getHostname(locatorHostUrl) + ":" + Parameters.getPort(locatorHostUrl));

			clientCache = new ClientCacheFactory().addPoolLocator(Parameters.getHostname(locatorHostUrl),
			        Parameters.getPort(locatorHostUrl)).create();

			LOG.info(clientCache);

			QueryService queryService = clientCache.getQueryService();

			LOG.info(queryService);

			SelectResults<Struct> results = (SelectResults<Struct>) queryService.newQuery(params.getOqlQuery())
			        .execute();

			LOG.info(results);

			resultIterator = results.iterator();

			LOG.info(resultIterator);
		} catch (Exception ex) {
			LOG.error(ex);
			throw ex;
		}

		return true;
	}

	@Override
	public OneRow readNextObject() throws Exception {

		LOG.info("NEXT Struct Call");

		if (resultIterator.hasNext()) {
			Struct nextStruct = resultIterator.next();
			LOG.info("NEXT Struct: " + nextStruct);
			oneRow.setKey(nextStruct.get(GF_KEY));
			oneRow.setData(nextStruct);
			return oneRow;
		} else {
			return null;
		}
	}

	@Override
	public void closeForRead() throws Exception {
		clientCache.close();
	}
}
