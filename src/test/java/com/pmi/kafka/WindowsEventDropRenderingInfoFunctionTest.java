package com.pmi.kafka;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class WindowsEventDropRenderingInfoFunctionTest {

    @Test
    void testDropRenderingInfo() throws Exception {

        var udf = new WindowsEventDropRenderingInfoFunction();

        var input = ResourceUtils.readResourceAsString("/windows.event.1.xml");

        var output = udf.eval(input);

        Assertions.assertThat(output)
                .isNotNull();

        Assertions.assertThat(output)
                .doesNotContain("RenderingInfo");

        System.out.println(output);


    }

}
