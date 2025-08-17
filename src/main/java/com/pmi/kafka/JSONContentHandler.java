package com.pmi.kafka;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * SAX {@link ContentHandler} that handles elements from namespace
 */
public class JSONContentHandler implements ContentHandler {

    public static final String NS = "http://xml.kafka.pmi.com/2025/json";

    private JSONContext jsonContext = new RootContext();

    public Object getJsonContent() {
        return jsonContext.getContent();
    }

    @Override
    public void setDocumentLocator(Locator locator) {

    }

    @Override
    public void startDocument() throws SAXException {

    }

    @Override
    public void endDocument() throws SAXException {

    }

    @Override
    public void startPrefixMapping(String prefix, String uri) throws SAXException {

    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {

    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        this.jsonContext = this.jsonContext.startElement(uri, localName, qName, atts);
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        this.jsonContext = this.jsonContext.endElement(uri, localName, qName);
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        this.jsonContext.characters(ch, start, length);
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
        // ignore
    }

    @Override
    public void processingInstruction(String target, String data) throws SAXException {
        // ignore
    }

    @Override
    public void skippedEntity(String name) throws SAXException {
        // ignore
    }

    // context stack

    protected static abstract class JSONContext {

        protected Object getContent() {
            throw new UnsupportedOperationException();
        }

        protected JSONContext startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            throw new SAXException("Element unsupported at this location: " + localName);
        }

        protected void characters(char[] ch, int start, int length) throws SAXException {
            throw new UnsupportedOperationException();
        }

        protected JSONContext endElement(String uri, String localName, String qName) throws SAXException {
            throw new SAXException();
        }

    }

    protected static abstract class ParentContext extends JSONContext {

        protected Object content = null;

        protected void setContent(Object content) {
            this.content = content;
        }

        protected Object getContent() {
            return this.content;
        }

        protected JSONContext startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if (!NS.equals(uri)) {
                throw new SAXException("Unsupported namespace: " + uri);
            }
            return switch (localName) {
                case "object" -> new ObjectContext(this);
                case "array" -> new ArrayContext(this);
                case "integer" -> new IntegerContext(this);
                case "string" -> new StringContext(this);
                case "float" -> new FloatContext(this);
                case "boolean" -> new BooleanContext(this);
                case "null" -> new NullContext(this);
                default -> throw new SAXException("Element unsupported at this location: " + localName);
            };
        }
    }

    protected static abstract class ChildContext<T> extends JSONContext {

        protected final ParentContext parent;

        protected ChildContext(ParentContext parent) {
            this.parent = parent;
        }

        @Override
        protected JSONContext endElement(String uri, String localName, String qName) throws SAXException {
            parent.setContent(getData());
            return parent;
        }

        protected abstract T getData();

    }

    protected static class RootContext extends ParentContext {

    }

    protected static abstract class TextContext<T> extends ChildContext<T> {

        protected final StringBuilder stringBuilder = new StringBuilder();

        protected TextContext(ParentContext parent) {
            super(parent);
        }

        @Override
        protected void characters(char[] ch, int start, int length) {
            stringBuilder.append(ch, start, length);
        }

    }

    protected static class NullContext extends ChildContext<Void> {

        protected NullContext(ParentContext parent) {
            super(parent);
        }

        @Override
        protected Void getData() {
            return null;
        }

    }

    protected static class StringContext extends TextContext<String> {

        protected StringContext(ParentContext parent) {
            super(parent);
        }

        @Override
        protected String getData() {
            return this.stringBuilder.toString();
        }

    }

    protected static class IntegerContext extends TextContext<BigInteger> {

        protected IntegerContext(ParentContext parent) {
            super(parent);
        }

        @Override
        protected BigInteger getData() {
            return new BigInteger(this.stringBuilder.toString().trim());
        }

    }

    protected static class FloatContext extends TextContext<BigDecimal> {

        protected FloatContext(ParentContext parent) {
            super(parent);
        }

        @Override
        protected BigDecimal getData() {
            return new BigDecimal(this.stringBuilder.toString().trim());
        }

    }

    protected static class BooleanContext extends TextContext<Boolean> {

        protected BooleanContext(ParentContext parent) {
            super(parent);
        }

        @Override
        protected Boolean getData() {
            return Boolean.valueOf(this.stringBuilder.toString().trim());
        }

    }

    protected static class ArrayContext extends ChildContext<List<Object>> {

        private final List<Object> elements = new LinkedList<>();

        protected ArrayContext(ParentContext parent) {
            super(parent);
        }

        protected void addElement(Object value) {
            this.elements.add(value);
        }

        @Override
        protected List<Object> getData() {
            return elements;
        }

        @Override
        protected JSONContext startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if (!NS.equals(uri)) {
                throw new SAXException("Unsupported namespace: " + uri);
            }
            if (!"element".equals(localName)) {
                throw new SAXException("Element unsupported at this location: " + localName);
            }
            return new ElementContext(this);
        }

    }

    protected static class ElementContext extends ParentContext {

        protected final ArrayContext parent;

        protected ElementContext(ArrayContext parent) {
            this.parent = parent;
        }

        @Override
        protected JSONContext endElement(String uri, String localName, String qName) throws SAXException {
            parent.addElement(content);
            return parent;
        }

    }

    protected static class ObjectContext extends ChildContext<Map<String,Object>> {

        protected final Map<String, Object> properties = new LinkedHashMap<>();

        protected ObjectContext(ParentContext parent) {
            super(parent);
        }

        @Override
        protected Map<String, Object> getData() {
            return properties;
        }

        protected void addProperty(@Nonnull String name, Object value) {
            properties.put(name, value);
        }

        @Override
        protected PropertyContext startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
            if (!NS.equals(uri)) {
                throw new SAXException("Unsupported namespace: " + uri);
            }
            if (!"property".equals(localName)) {
                throw new SAXException("Element unsupported at this location: " + localName);
            }
            var name = atts.getValue("name");
            if (name == null) {
                throw new SAXException("Missing mandatory attribute: name");
            }
            return new PropertyContext(this, name);
        }

    }

    protected static class PropertyContext extends ParentContext {

        protected final String name;

        protected final ObjectContext parent;

        protected PropertyContext(ObjectContext parent, String name) {
            this.parent = parent;
            this.name = name;
        }

        @Override
        protected JSONContext endElement(String uri, String localName, String qName) throws SAXException {
            parent.addProperty(name, getContent());
            return parent;
        }

    }

}
