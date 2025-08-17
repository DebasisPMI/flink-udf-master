package com.pmi.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;
import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

//@FunctionHint(
//        arguments = {
//                @ArgumentHint(type = @DataTypeHint("BYTES"), name = "eventXml", isOptional = true)
//        },
//        output = @DataTypeHint("STRING")
//)
//@FunctionHint(
//        arguments = {
//                @ArgumentHint(type = @DataTypeHint("STRING"), name = "eventXml", isOptional = true)
//        },
//        output = @DataTypeHint("STRING")
//)
public class WindowsEventToJSONFunction extends ScalarFunction {

    private static final Logger logger = LogManager.getLogger();
//
//    public String eval(byte[] eventXml) {
//
//        if (eventXml == null) {
//            return null;
//        }
//
//        try {
//
//            var reader = new ByteArrayInputStream(eventXml);
//            var writer = new JSONContentHandler();
//
//            FunctionHelper.transformer.accept(
//                    new StreamSource(reader),
//                    new SAXResult(writer)
//            );
//
//            return new ObjectMapper().writeValueAsString(writer.getJsonContent());
//
//        } catch (Exception e) {
//            logger.error("Unable to convert the XML to JSON", e);
//            return null;
//        }
//
//    }

    public String eval(String eventXml) {

        if (eventXml == null) {
            return null;
        }

        try {

            var reader = new StringReader(eventXml);
            var writer = new JSONContentHandler();

            FunctionHelper.transformer.accept(
                    new StreamSource(reader),
                    new SAXResult(writer)
            );

            var om = new ObjectMapper();
            om.enable(SerializationFeature.INDENT_OUTPUT);
            return om.writeValueAsString(writer.getJsonContent());

        } catch (Exception e) {
            logger.error("Unable to convert the XML to JSON", e);
            return null;
        }

    }

    /**
     * <a href="https://www.baeldung.com/java-bill-pugh-singleton-implementation">Bill Pugh singleton implementation</a>
     */
    private static class FunctionHelper {

        private static final BiConsumer<Source, Result> transformer = XsltFunctionFactory.newInstance()
                .createXsltTransformer(WindowsEventToJSONFunction.class.getResourceAsStream("WindowsEventToJSON.xslt"));

    }

}
