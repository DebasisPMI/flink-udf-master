package com.pmi.kafka;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;


/**
 * Optimized version of {@link WindowsEventDropRenderingInfoFunction}. This function delegates the XSLT processing code
 * to general purpose helper function and caches the function to reuse the parsed transformation.
 * In addition, it provides type hints if one wanted to implement polymorphic function.
 * <p>
 * Overloading the signature allows the function to be used as:
 * <pre>INSERT INTO `sentinel-0` SELECT key, WindowsEventDropRenderingInfo(val) FROM `cloudwatch-0`;</pre>
 *
 * @author <a href="mailto:piotr.smolinski@confluent.io">Piotr Smolinski</a>
 */
//@FunctionHint(
//        arguments = {
//                @ArgumentHint(type = @DataTypeHint("BYTES"), name = "eventXml")
//        },
//        output = @DataTypeHint("BYTES")
//)
//@FunctionHint(
//        arguments = {
//                @ArgumentHint(type = @DataTypeHint("STRING"), name = "eventXml")
//        },
//        output = @DataTypeHint("STRING")
//)
public class WindowsEventDropRenderingInfoOptimized extends ScalarFunction {

    private static final Logger logger = LogManager.getLogger();

    public byte[] eval(byte[] xml) {

        if (xml == null) {
            return null;
        }

        try {
            String out = FunctionHelper.transformation.apply(new String(xml, StandardCharsets.UTF_8));
            if (out == null) {
                return null;
            }
            return out.getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.warn("Failed transforming xml", e);
            return null;
        }

    }

    public String eval(String xml) {

        if (xml == null) {
            return null;
        }

        try {
            return FunctionHelper.transformation.apply(xml);
        } catch (Exception e) {
            logger.warn("Failed transforming xml", e);
            return null;
        }

    }

    /**
     * <a href="https://www.baeldung.com/java-bill-pugh-singleton-implementation">Bill Pugh singleton implementation</a>.
     * Smart way of lazy-creation of singletons without locking. Nowadays dated.
     */
    private static class FunctionHelper {
        private static final Function<String,String> transformation =
                XsltFunctionFactory.newInstance()
                        .createXsltFunction(WindowsEventDropRenderingInfoFunction.class.getResourceAsStream(
                                "WindowsEventDropRenderingInfo.xslt"));

    }

}
