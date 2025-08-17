package com.pmi.kafka;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ParseCloudTrailFunctionTest {

    @Test
    void testParsingRecord() throws Exception {

        var udf = new ParseCloudTrailFunction();

        var row = udf.eval(ResourceUtils.readResourceAsBytes("/cloudtrail.record.0.json"));

        Assertions.assertThat(row).isNotNull();

    }

    @Test
    void testTypeInference() throws Exception {

        var udf = new ParseCloudTrailFunction();

//        var factory = Mockito.mock(DataTypeFactory.class);
//        udf.getTypeInference(null);

    }

}
