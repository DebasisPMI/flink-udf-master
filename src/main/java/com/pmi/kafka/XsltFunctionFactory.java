package com.pmi.kafka;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class XsltFunctionFactory {

    public static XsltFunctionFactory newInstance() {
        return new XsltFunctionFactory();
    }

    public Function<String,String> createXsltFunction(InputStream xslt) {

        try {

            var transformer = createXsltTransformer(xslt);

            return xml -> {

                try {

                    var reader = new StringReader(xml);
                    var writer = new StringWriter();

                    transformer.accept(new StreamSource(reader), new StreamResult(writer));

                    return writer.toString();

                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            };

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public BiConsumer<Source, Result> createXsltTransformer(InputStream xslt) {

        try {

            // templates object is XSLT parsing result, it is immutable
            var templates = TransformerFactory.newInstance()
                    .newTemplates(new StreamSource(xslt));

            return (source,result) -> {

                try {

                    // transformer is runtime state, it is safe to use multiple times in one thread
                    var transformer = templates.newTransformer();

                    transformer.transform(source, result);

                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            };

        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
