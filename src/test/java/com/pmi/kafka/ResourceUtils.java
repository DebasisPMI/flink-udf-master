package com.pmi.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ResourceUtils {

    static String readResourceAsString(String name) throws IOException {
        return new String(readResourceAsBytes(name), StandardCharsets.UTF_8);
    }

    static byte[] readResourceAsBytes(String name) throws IOException {
        var ostream = new ByteArrayOutputStream();
        try (var istream = ResourceUtils.class.getResourceAsStream(name)) {
            var buffer = new byte[8192];
            for (;;) {
                int r = istream.read(buffer);
                if (r<0) break;
                ostream.write(buffer, 0, r);
            }
        }
        return ostream.toByteArray();
    }

}
