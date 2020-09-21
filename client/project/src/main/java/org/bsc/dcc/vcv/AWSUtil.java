package org.bsc.dcc.vcv;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

public class AWSUtil {
	
	private static final Logger logger = LogManager.getLogger("AllLog");

    public static String getValue(String secretName) {
        String retVal = null;
    	try {
        	Region region = Region.US_WEST_2;
            SecretsManagerClient secretsClient = SecretsManagerClient.builder()
                    .region(region)
                    .build();
        	logger.info("Retrieving secret: " + secretName);
            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                .secretId(secretName)
                .build();
            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            String secretJson = valueResponse.secretString();
            JSONObject secretJsonObj = new JSONObject(secretJson);
            String secret = secretJsonObj.getString(secretName);
            retVal = secret;
        }
        catch (SecretsManagerException e) {
        	logger.error("Error in AWSUtil getValue.");
			logger.error(e);
			logger.error(AppUtil.stringifyStackTrace(e));
        	System.err.println(e.awsErrorDetails().errorMessage());
            logger.error(e);
            e.printStackTrace();
        }
    	return retVal;
    }
}


