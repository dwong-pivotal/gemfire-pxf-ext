package io.pivotal.pxf.plugins.gemfire;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.pxf.api.Fragment;
import com.pivotal.pxf.api.Fragmenter;
import com.pivotal.pxf.api.utilities.InputData;

public class GemfireFragmenter extends Fragmenter {

	private static final String URL_HOST = "URL-HOST";

	private static final Log LOG = LogFactory.getLog(GemfireFragmenter.class);

	private Parameters params;

	public GemfireFragmenter(InputData metaData) throws MissingArgumentException {
		super(metaData);
		params = new Parameters(metaData);
	}

	@Override
	public List<Fragment> getFragments() throws Exception {

		String pxfHost = inputData.getUserProperty(URL_HOST);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Locators:" + params.getLocators()[0] + ", PXF host:" + pxfHost);
		}

		List<Fragment> fragments = new ArrayList<Fragment>();

		fragments.add(new Fragment(inputData.getDataSource(), new String[] { pxfHost }, new byte[0]));

		return fragments;
	}
}
