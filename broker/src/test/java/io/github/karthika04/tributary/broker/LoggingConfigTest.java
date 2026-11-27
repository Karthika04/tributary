package io.github.karthika04.tributary.broker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Logback's configuration: a mistake in it would not fail any other test, only make the
 * broker's logs disappear, so it is checked directly.
 */
class LoggingConfigTest {

    @Test
    void textByDefaultJsonOnRequest() throws Exception {
        Document doc;
        try (InputStream in = BrokerServer.class.getResourceAsStream("/logback.xml")) {
            assertNotNull(in, "logback.xml is missing from the broker's resources");
            doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(in);
        }
        Map<String, String> encoderByAppender = new HashMap<>();
        NodeList appenders = doc.getElementsByTagName("appender");
        for (int i = 0; i < appenders.getLength(); i++) {
            Element appender = (Element) appenders.item(i);
            Element encoder = (Element) appender.getElementsByTagName("encoder").item(0);
            encoderByAppender.put(appender.getAttribute("name"), encoder.getAttribute("class"));
        }
        assertEquals("", encoderByAppender.get("TEXT")); // the default pattern encoder
        assertEquals("ch.qos.logback.classic.encoder.JsonEncoder", encoderByAppender.get("JSON"));

        Element root = (Element) doc.getElementsByTagName("root").item(0);
        assertEquals("${TRIBUTARY_LOG_LEVEL:-INFO}", root.getAttribute("level"));
        Element ref = (Element) root.getElementsByTagName("appender-ref").item(0);
        assertEquals("${TRIBUTARY_LOG_FORMAT:-TEXT}", ref.getAttribute("ref"));
    }
}
