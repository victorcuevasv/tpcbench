/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.bsc.dcc.vcv;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetSecretValue {
	
	private static final Logger logger = LoggerFactory.getLogger("AllLog");

    public static void main(String[] args) {

        final String USAGE = "\n" +
                "To run this example, supply the name of the secret (for example, tutorials/MyFirstSecret).  \n" +
                "\n" +
                "Example: GetSecretValue <secretName>\n";

        if (args.length < 1) {
            System.out.println(USAGE);
            System.exit(1);
        }

        /* Read the name from command args */
        String secretName = args[0];

        Region region = Region.US_WEST_2;
        SecretsManagerClient secretsClient = SecretsManagerClient.builder()
                .region(region)
                .build();

        getValue(secretsClient, secretName);
    }

    public static void getValue(SecretsManagerClient secretsClient,String secretName) {

        try {
        	logger.info("Retrieving secret: " + secretName);
            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                .secretId(secretName)
                .build();

            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            String secret = valueResponse.secretString();
            System.out.println(secret);

        } catch (SecretsManagerException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            logger.error(e.awsErrorDetails().errorMessage(), e);
            System.exit(1);
        }
    }
}


