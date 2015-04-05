package io.pivotal.pxf.plugins.gemfire;

import static org.apache.commons.lang.StringUtils.isBlank;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pivotal.pxf.api.utilities.InputData;

public class Parameters {

	private static final Log LOG = LogFactory.getLog(Parameters.class);

	public static String LOCATORS_PARAM = "LOCATORS";
	public static String OQL_QUERY_PARAM = "QUERY";
	public static String OQL_REGION_PARAM = "REGION";

	private String[] locators = null;
	private String oqlQuery = null;
	private String oqlRegion;

	public Parameters(InputData inputData) throws MissingArgumentException {

		String locatorsString = inputData.getUserProperty(Parameters.LOCATORS_PARAM);

		LOG.info(Parameters.LOCATORS_PARAM + ":" + locatorsString);

		if (!isBlank(locatorsString)) {
			locators = locatorsString.split(",");
		} else {
			throw new MissingArgumentException(Parameters.LOCATORS_PARAM + " is not defined.");
		}

		oqlQuery = inputData.getUserProperty(Parameters.OQL_QUERY_PARAM);
		oqlRegion = inputData.getUserProperty(Parameters.OQL_REGION_PARAM);

		LOG.info(Parameters.OQL_QUERY_PARAM + ":" + oqlQuery);
		LOG.info(Parameters.OQL_REGION_PARAM + ":" + oqlRegion);

		if (isBlank(oqlQuery) && isBlank(oqlRegion)) {
			throw new MissingArgumentException("Either " + Parameters.OQL_QUERY_PARAM + " or "
			        + Parameters.OQL_REGION_PARAM + " has to be defined.");
		}

		// REGION overrides the QUERY parameters
		if (!isBlank(oqlRegion)) {
			oqlQuery = String.format("SELECT e.key, e.value FROM /%s.entrySet e", oqlRegion.trim());
		}
	}

	public String[] getLocators() {
		return locators;
	}

	public String getOqlQuery() {
		return oqlQuery;
	}

	public static int getPort(String hostUrl) {
		return Integer.valueOf(hostUrl.split(":")[1]).intValue();
	}

	public static String getHostname(String hostUrl) {
		return hostUrl.split(":")[0].trim();
	}
}
