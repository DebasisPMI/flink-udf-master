package com.pmi.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;
import java.util.function.Function;

public class WindowsEventDropRenderingInfoTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

    private Function<String,String> transform;

    @Override
    public void configure(Map<String, ?> configs) {

        this.transform = XsltFunctionFactory.newInstance()
                .createXsltFunction(WindowsEventDropRenderingInfoFunction.class.getResourceAsStream(
                        "WindowsEventDropRenderingInfo.xslt"));

    }

    @Override
    public R apply(R connectRecord) {

        if (connectRecord.valueSchema() != null && connectRecord.valueSchema().type() != Schema.Type.STRING) {
            throw new ConnectException("Typed value schema must be STRING. Got: " + connectRecord.valueSchema());
        }

        if (connectRecord.value() == null) {
            return connectRecord;
        }

        if (!(connectRecord.value() instanceof String)) {
            throw new ConnectException("String value expected. Got: " + connectRecord.value().getClass());
        }

        return connectRecord.newRecord(
                connectRecord.topic(),
                connectRecord.kafkaPartition(),
                connectRecord.keySchema(),
                connectRecord.key(),
                connectRecord.valueSchema(),
                transform.apply((String)connectRecord.value()),
                connectRecord.timestamp(),
                connectRecord.headers()
        );

    }

    private static ConfigDef CONFIG_DEF = buildConfigDef();


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        this.transform = null;
    }

    private static ConfigDef buildConfigDef() {
        return new ConfigDef();
    }

}
