package org.hsqldb.lib;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides input values for search algorithm test cases.
 * 
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
public enum SearchAlgorithmProperty {
    SearchTarget("search.target"),
    SearchPatternSuccess("search.pattern.success"),
    SearchPatternFail("search.pattern.fail"),
    SearchPatternEmpty(""),
    SearchPatternNull(null);
    private static Logger LOG;
    private static Properties PROPERTIES;
    private static final String RESOURCE
            = "resources/search.algorithm.test.properties";

    private static int indexOf(final byte[] data, final byte[] pattern) {
        for (int i = 0; i < data.length - pattern.length + 1; ++i) {
            boolean found = true;
            for (int j = 0; j < pattern.length; ++j) {
                if (data[i + j] != pattern[j]) {
                    found = false;
                    break;
                }
            }
            if (found) {
                return i;
            }
        }
        return -1;
    }
    public final long expectedBytePosition;
    public final long expectedCharPosition;
    public final String value;

    SearchAlgorithmProperty(final String key) {
        value = key == null
                ? null
                : key.isEmpty()
                ? key
                : getProperties().getProperty(key);
        expectedCharPosition = key == null
                ? -1L
                : key.isEmpty() || Objects.equals(key, "search.target")
                ? 0L
                : getProperties().getProperty("search.target").indexOf(value);
        expectedBytePosition = key == null
                ? -1
                : key.isEmpty() || Objects.equals(key, "search.target")
                ? 0L
                : indexOf(
                        getProperties().getProperty("search.target").getBytes(),
                        value.getBytes());
    }
    
    private Logger logger() {
        if (LOG == null) {
            LOG = Logger.getLogger(SearchAlgorithmProperty.class.getName());
        }
        return LOG;
    }

    @SuppressWarnings("ReturnOfCollectionOrArrayField")
    private Properties getProperties() {
        if (PROPERTIES == null) {
            final Properties properties = new Properties();
            PROPERTIES = properties;
            final URL resource = getClass().getResource(RESOURCE);
            if (resource == null) {
                logger().log(Level.SEVERE, RESOURCE + " does not exist.");
            } else {
                try (final InputStream stream = resource.openStream()) {
                    properties.load(stream);
                } catch (IOException ex) {
                    logger().log(Level.SEVERE, RESOURCE, ex);
                }
            }
        }
        return PROPERTIES;
    }
}
