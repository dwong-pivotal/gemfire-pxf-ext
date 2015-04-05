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

	private static final Log LOG = LogFactory.getLog(GemfireFragmenter.class);

	private Parameters params;

	public GemfireFragmenter(InputData metaData) throws MissingArgumentException {
		super(metaData);
		params = new Parameters(metaData);
	}

	// "phd1:10334"
	
	@Override
	public List<Fragment> getFragments() throws Exception {
		List<Fragment> fragments = new ArrayList<Fragment>();
		String host = params.getLocators()[0];
		
		LOG.info("Host:" + host);
		
//		fragments.add(new Fragment(host, new String[] { host.contains(":") ? host.substring(0, host.indexOf(":"))
//		        : host }, new byte[0]));
		fragments.add(new Fragment("phd1", new String[] { "phd1" }, new byte[0]));
		
		return fragments;
	}

}
