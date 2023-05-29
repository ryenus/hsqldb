package org.hsqldb.lib;

import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(ScannerSearchAlgorithm.class)
public class ScannerSearchAlgorithmTest extends BaseTestCase {

    private static final Logger LOG
            = Logger.getLogger(ScannerSearchAlgorithmTest.class.getName());

    /**
     * Constructs a new test case with the given {@code testName}.
     * 
     * @param testName The name of the test case.
     */
    public ScannerSearchAlgorithmTest(final String testName) {
        super(testName);
    }

    /**
     * Test of search method, of class ScannerSearchAlgorithm.
     */
    @OfMethod("search(java.io.Reader,char[],boolean)")
    public void testSearchReaderForCharArrayPattern() {
        final boolean literal = true;
        final SearchAlgorithmProperty[] values 
                = SearchAlgorithmProperty.values();
        for (final SearchAlgorithmProperty sap : values) {
            final Reader reader = new StringReader(
                    SearchAlgorithmProperty.SearchTarget.value);
            final char[] searchstr = sap.value == null 
                    ? null 
                    : sap.value.toCharArray();
            final long expected = sap.expectedCharPosition;
            final long actual = ScannerSearchAlgorithm.search(reader, searchstr,
                    literal);
            final String msg = "position of " + sap.name() 
                    + " in reader stream";
            assertEquals(msg, expected, actual);
        }
    }

    /**
     * Test of search method, of class ScannerSearchAlgorithm.
     */
    @OfMethod("search(java.io.Reader,java.util.regex.Pattern)")
    public void testSearchReaderForRegexPattern() {
        final SearchAlgorithmProperty[] values
                = SearchAlgorithmProperty.values();
        for (final SearchAlgorithmProperty sap : values) {
            final Reader reader = new StringReader(
                    SearchAlgorithmProperty.SearchTarget.value);
            final Pattern pattern = sap.value == null 
                    ? null 
                    : Pattern.compile(Pattern.quote(sap.value));
            final long expected = sap.expectedCharPosition;
            final long actual = ScannerSearchAlgorithm.search(reader,
                    pattern);
            final String msg = "position of " + sap.name()
                    + " in reader stream";
            assertEquals(msg, expected, actual);
        }
    }

    /**
     * Test of search method, of class ScannerSearchAlgorithm.
     */
    @OfMethod("search(java.io.Reader,java.lang.String,boolean)")
    public void testSearchReaderForStringPattern() {
        final boolean literal = true;
        final SearchAlgorithmProperty[] values
                = SearchAlgorithmProperty.values();
        for (final SearchAlgorithmProperty sap : values) {
            final Reader reader = new StringReader(
                    SearchAlgorithmProperty.SearchTarget.value);
            final String searchstr = sap.value;
            final long expected = sap.expectedCharPosition;
            final long actual = ScannerSearchAlgorithm.search(reader, searchstr,
                    literal);
            final String msg = "position of " + sap.name() 
                    + " in reader stream";
            assertEquals(msg, expected, actual);
        }
    }
}
