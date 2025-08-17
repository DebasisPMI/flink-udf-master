package com.pmi.kafka;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WindowsEventToJSONFunctionTest {

    @Test
    void testConvertToJson() throws Exception {

        var udf = new WindowsEventToJSONFunction();

        var xml = ResourceUtils.readResourceAsString("/windows.event.1.xml");

        var json = udf.eval(xml);

        System.out.println(json);

        var om = new ObjectMapper();

        var parsed = om.readValue(json, Object.class);

        Assertions.assertThat(parsed)
                        .isNotNull();


    }

}
