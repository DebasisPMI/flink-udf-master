package com.pmi.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

/**
 *
 * Function registration:
 * <pre>CREATE FUNCTION ParseCloudTrail AS 'com.pmi.kafka.ParseCloudTrailFunction'
 * USING JAR 'confluent-artifact://cfa-r5z7p1';</pre>
 *
 * Routing query:
 * <pre>INSERT INTO `sentinel-0`
 * SELECT key, val FROM `cloudtrail-0`
 * where ParseCloudTrail(val).eventSource in (
 *   'organizations.amazonaws.com',
 *   'config.amazonaws.com',
 *   'cloudtrail.amazonaws.com',
 *   'ec2.amazonaws.com',
 *   'apigateway.amazonaws.com',
 *   'elasticloadbalancing.amazonaws.com',
 *   'signin.amazonaws.com',
 *   'iam.amazonaws.com',
 *   'rds.amazonaws.com',
 *   's3.amazonaws.com',
 *   'redshift.amazonaws.com',
 *   'dynamodb.amazonaws.com',
 *   'lambda.amazonaws.com'
 * );</pre>
 *
 * Select and flatten fields:
 * <pre>select json.* from (select ParseCloudTrail(val) json FROM `cloudtrail-0`);</pre>
 *
 */
@FunctionHint(
        output = @DataTypeHint("ROW<`eventTime` TIMESTAMP_LTZ, `eventSource` STRING, `eventName` STRING, `eventType` STRING>")
)
public class ParseCloudTrailFunction extends ScalarFunction {

    private static final Logger logger = LogManager.getLogger();

    public Row eval(byte[] recordJson) {

        if (recordJson == null) {
            return null;
        }

        return eval(new String(recordJson, StandardCharsets.UTF_8));

    }

    public Row eval(String recordJson) {

        if (recordJson == null) {
            return null;
        }

        try {

            ObjectMapper om = new ObjectMapper();

            Map<?,?> record = (Map<?,?>)om.readValue(recordJson, Object.class);

            return Row.of(
                    Instant.parse((String)record.get("eventTime")),
                    (String)record.get("eventSource"),
                    (String)record.get("eventName"),
                    (String)record.get("eventType")
            );


        } catch (Exception e) {
            logger.error("Unable to transform record JSON", e);
            return null;
        }

    }

}
