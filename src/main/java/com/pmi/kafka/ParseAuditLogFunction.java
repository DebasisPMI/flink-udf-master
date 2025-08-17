package com.pmi.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * UDF to parse linux audit log lines.
 * <p>
 * The audit log format is as follows:
 * <ul>
 *     <li>type=&lt;type&gt;</li>
 *     <li>whitespace</li>
 *     <li>msg=audit(&lt;timestamp&gt;:&lt;uniqueId&gt;)</li>
 *     <li>colon</li>
 *     <li>whitespace separated sequence of &lt;key&gt;=&lt;value&gt; or &lt;key&gt;="&lt;value&gt;" pairs</li>
 * </ul>
 */
@FunctionHint(
        output = @DataTypeHint(
                "ROW<`json` STRING, `errorClass` STRING, `errorMessage` STRING, `errorStackTrace` STRING>")
)

public class ParseAuditLogFunction extends ScalarFunction {

    private static final Logger log = LoggerFactory.getLogger(ParseAuditLogFunction.class);
    private static final Pattern pattern = Pattern.compile(
            "type=(?<type>[A-Z0-9_]+)\\s+" +
                    "msg=audit\\((?<timestampSec>\\d+)\\.(?<timestampMillis>\\d{3}):(?<uniqueId>\\d+)\\):\\s+(?<attributes>.*)");

    private static final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public Row eval(String hostname, String line) {
        if (line == null || line.isBlank()) {
            return errorRow("EMPTY_INPUT", null, "Input line is null or blank", null);
        }
        try {
            String jsonStr = parse(hostname, line);
            if(jsonStr == null){
                return errorRow("MALFORMED_INPUT", null, "Message did not match the expected format", null);
            }
            return Row.of(jsonStr, null, null, null, null);
        } catch (Exception e) {
            return errorRow("PARSING_ERROR", e.getClass().getName(), e.getMessage(), e);
        }
    }

    public String parse(String hostname, String line) throws Exception {

        line = Objects.requireNonNull(line).trim();

        Matcher matcher = pattern.matcher(line);

        if (!matcher.matches()) {
            log.warn("Malformed line - no matching regex : {}", snippet(line));
            return null;
        }

        String type = matcher.group("type");
        String timestampSec = matcher.group("timestampSec");
        String timestampMillis = matcher.group("timestampMillis");
        String uniqueId = matcher.group("uniqueId");
        String attributes = matcher.group("attributes");

        Map<String,String> attributesMap = new LinkedHashMap<>();

        try{
            ParseState state = separator(attributesMap, line);
            int pos = 0;
            for (char c : attributes.toCharArray()) {
                state = state.parse(c, pos++);
            }
            state.parse(-1, pos);
        }catch(IllegalArgumentException iae){
            log.debug("Failed parsing due to illegal argument passed : {}", snippet(line), iae);
            throw iae;
        }

        Map<String, Object> result = new LinkedHashMap<>();

        if (hostname != null) {
            result.put("hostname", hostname);
        }
        result.put("type", type);
        result.put("timestamp", Long.parseLong(timestampSec+timestampMillis));
        result.put("uniqueId", Integer.parseInt(uniqueId));
        result.put("attributes", attributesMap);

        try{
            return mapper.writeValueAsString(result);
        }catch(Exception ex){
            log.error("JSON serialization failed for line: {}", snippet(line), ex);
            throw ex;
        }
    }

    // --- Error Row Builder ---
    private Row errorRow(String errorType, String errorClass, String message, Exception e) {
        String stackTrace = null;
        if (e != null) {
            StringWriter w = new StringWriter();
            e.printStackTrace(new PrintWriter(w));
            stackTrace = w.toString();
        }
        return Row.of(null, errorType, errorClass, message, stackTrace);
    }

    private String snippet(String line) {
        return line.length() > 200 ? line.substring(0, 200) + "..." : line;
    }

    // state machine callbacks

    interface ParseState {
        ParseState parse(int character, int pos);
    }

    static ParseState done() {
        return (ch, pos) -> {
            throw new IllegalArgumentException("Parsing Completed unexpectedly at position, {}"+pos);
        };
    }

    static ParseState separator(Map<String,String> map, String line) {
        return (ch, pos) -> switch(ch){
            case -1 -> done();
            default -> {
                if(Character.isWhitespace(ch)){
                    yield separator(map, line);
                }else if(Character.isAlphabetic(ch) || ch == '_' || Character.isDigit(ch)){
                    yield key(map, new StringBuilder(Character.toString(ch)));
                }else{
                    throw new IllegalArgumentException("Unexpected separator character: " + (char)ch+",at position :"+pos);
                }
            }
        };
    }

    static ParseState key(Map<String,String> map, StringBuilder key) {
        return (ch, pos) -> switch(ch) {
            case -1 -> throw new IllegalArgumentException("Unexpected end with key ::"+key);
            case '=' -> value(map, key.toString());
            default ->  {
                if(Character.isAlphabetic(ch) || ch == '_' || Character.isDigit(ch)){
                    yield key(map, key.append((char) ch));
                }else{
                    throw new IllegalArgumentException("Unexpected key character: " + (char)ch+",at position :"+pos);
                }
            }
        };
    }

    static ParseState value(Map<String, String> map, String key) {
        return (ch, pos) -> switch (ch) {
            case -1 -> {
                map.put(key, "");
                yield done();
            }
            case '"' -> valueQuoted(map, key, new StringBuilder(), '"');
            case '\'' -> valueQuoted(map, key, new StringBuilder(), '\'');
            default -> {
                if (Character.isWhitespace(ch)) {
                    map.put(key, "");
                    yield separator(map, "");
                } else {
                    yield valuePlain(map, key, new StringBuilder(Character.toString((char) ch)));
                }
            }
        };
    }

    static ParseState valuePlain(Map<String, String> map, String key, StringBuilder value) {
        return (ch, pos) -> switch (ch) {
            case -1 -> {
                map.put(key, value.toString());
                yield done();
            }
            default -> {
                if (Character.isWhitespace(ch)) {
                    map.put(key, value.toString());
                    yield separator(map, "");
                } else if (ch == '"') {
                    throw new IllegalArgumentException("Unexpected double quote inside value at pos :" + pos);
                } else {
                    yield valuePlain(map, key, value.append((char) ch));
                }
            }
        };
    }

    static ParseState valueQuoted(Map<String, String> map, String key, StringBuilder value, char quoteChar) {
        return (ch, pos) -> switch (ch) {
            case -1 -> throw new IllegalArgumentException("Premature end of line inside quoted value for key :" + key);
            default -> {
                if (ch == quoteChar) {
                    map.put(key, value.toString());
                    yield separator(map, "");
                } else {
                    yield valueQuoted(map, key, value.append((char) ch), quoteChar);
                }
            }
        };
    }

}
