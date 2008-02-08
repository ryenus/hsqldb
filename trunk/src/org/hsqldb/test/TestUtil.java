package org.hsqldb.test;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.FileNotFoundException;
import java.io.LineNumberReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;

import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.StringUtil;

/**
 * Utility class providing methodes for submitting test statements or
 * scripts to the database, comparing the results returned with
 * the expected results. The test script format is compatible with existing
 * scripts.
 *
 * @author ewanslater@users
 * @author fredt@users
 */
public class TestUtil {
    /* This class writes \n's as line delimiters.
     * It should write System.getProperty("line.separateor").
     *
     * The executing scripts do have state.  This class should be
     * redesigned with OOD.
     */
    static private final SimpleDateFormat sdfYMDHMS =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static private boolean abortOnErr = false;

    static final private String TIMESTAMP_VAR_STR = "${timestamp}";

    /**
     * Expand occurrences of "${timestamp}" in input to time stamps.
     */
    static protected void expandStamps(StringBuffer sb) {
        int i = sb.indexOf(TIMESTAMP_VAR_STR);
        if (i < 1) {
            return;
        }
        String timestamp = sdfYMDHMS.format(new java.util.Date());
        while (i > -1) {
            sb.replace(i, i + TIMESTAMP_VAR_STR.length(), timestamp);
            i = sb.indexOf(TIMESTAMP_VAR_STR);
        }
    }

    static void testScript(Connection aConnection, String aPath) {
        /*
         * This is a legacy wrapper method which purposefully inherits the sins
         * of the original.
         * No indication is given to the invoker of even RuntimeExceptions.
         */
        File file = new File(aPath);
        try {
            TestUtil.testScript(aConnection, file.getAbsolutePath(),
                    new FileReader(file));
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("test script file error: " + e.getMessage());
        }
    }

    /**
     * Runs a preformatted script.<p>
     *
     * Where a result set is required, each line in the script will
     * be interpreted as a seperate expected row in the ResultSet
     * returned by the query.  Within each row, fields should be delimited
     * using either comma (the default), or a user defined delimiter
     * which should be specified in the System property TestUtilFieldDelimiter
     * @param aConnection Connection object for the database
     * @param sourceName Identifies the script which failed
     * @param inReader Source of commands to be tested
     */
    public static void testScript(Connection aConnection, String sourceName,
            Reader inReader) throws SQLException, IOException {
        Statement statement = aConnection.createStatement();
        LineNumberReader reader = new LineNumberReader(inReader);
        HsqlArrayList section = null;

        System.out.println("Opened test script file: " + sourceName);

        /**
         * we read the lines from the start of one section of the script "/*"
         *  until the start of the next section, collecting the lines
         *  in the Vector lines.
         *  When a new section starts, we will pass the vector of lines
         *  to the test method to be processed.
         */
        int startLine = 1;

        try {

            while (true) {
                boolean startSection = false;
                String  line         = reader.readLine();

                if (line == null) {
                    break;
                }

                line = line.substring(
                    0, org.hsqldb.lib.StringUtil.rTrimSize(line));

                //if the line is blank or a comment, then ignore it
                if ((line.length() == 0) || line.startsWith("--")) {
                    continue;
                }

                //...check if we're starting a new section...
                if (line.startsWith("/*")) {
                    startSection = true;
                }

                if (line.charAt(0) != ' ' && line.charAt(0) != '*') {
                    startSection = true;
                }

                if (startSection) {

                    //...if we are, test the previous section (if it exists)...
                    if (section != null) {
                        testSection(statement, section, sourceName, startLine);
                    }

                    //...and then start a new section...
                    section   = new HsqlArrayList();
                    startLine = reader.getLineNumber();
                }

                section.add(line);
            }

            //send the last section for testing
            if (section != null) {
                testSection(statement, section, sourceName, startLine);
            }

            statement.close();

        // The following catch blocks are just to report the source location
        // of the failure.
        } catch (IOException ioe) {
            System.err.println("Error encountered at command beginning at "
                    + sourceName + ':' + startLine);
            throw ioe;
        } catch (SQLException se) {
            System.err.println("Error encountered at command beginning at "
                    + sourceName + ':' + startLine);
            throw se;
        } catch (RuntimeException re) {
            System.err.println("Error encountered at command beginning at "
                    + sourceName + ':' + startLine);
            throw re;
        }
        System.out.println("Processed " + reader.getLineNumber()
                + " lines from " + sourceName);
    }

    /** Legacy wrapper */
    static void test(Statement stat, String s, int line) {
        TestUtil.test(stat, s, null, line);
    }

    /**
     * Performs a preformatted statement or group of statements and throws
     *  if the result does not match the expected one.
     * @param line start line in the script file for this test
     * @param stat Statement object used to access the database
     * @param sourceName Identifies the script which failed
     * @param s Contains the type, expected result and SQL for the test
     */
    static void test(Statement stat, String s, String sourceName, int line) {

        //maintain the interface for this method
        HsqlArrayList section = new HsqlArrayList();

        section.add(s);
        testSection(stat, section, sourceName, line);
    }

    /**
     * Method to save typing ;-)
     * This method does not distinguish between normal and error output.
     *
     * @param s String to be printed
     */
    static void print(String s) {
        System.out.println(s);
    }

    /**
     * Takes a discrete section of the test script, contained in the
     * section vector, splits this into the expected result(s) and
     * submits the statement to the database, comparing the results
     * returned with the expected results.
     * If the actual result differs from that expected, or an
     * exception is thrown, then the appropriate message is printed.
     * @param stat Statement object used to access the database
     * @param section Vector of script lines containing a discrete
     * section of script (i.e. test type, expected results,
     * SQL for the statement).
     * @param line line of the script file where this section started
     */
    private static void testSection(Statement stat, HsqlArrayList section,
                                    String scriptName, int line) {

        //create an appropriate instance of ParsedSection
        ParsedSection pSection = parsedSectionFactory(section);

        if (pSection == null) {    //it was not possible to sucessfully parse the section
            System.err.println("The section starting at " + scriptName + ':'
                + line + " could not be parsed, and so was not processed.\n");
            return;
        }
        if (pSection instanceof IgnoreParsedSection) {
            System.out.println("At " + scriptName + ':' + line + ": "
                    + pSection.getResultString());
            return;
        }
        if (pSection instanceof DisplaySection
                || pSection instanceof WaitSection
                || pSection instanceof ProceedSection) {
            String s = pSection.getResultString();
            if (s != null) {
                // May or may not want to report line number for these sections?
                System.out.println(pSection.getResultString());
            }
        }
        if (pSection instanceof DisplaySection) {
            return; // Do not run test method for DisplaySections.
        }
        if (!pSection.test(stat)) {
            System.err.println("Section starting at " + scriptName + ':'
                    + line + " returned an unexpected result: " + pSection);
            if (TestUtil.abortOnErr) {
                throw new TestRuntimeException(scriptName + ": " + line
                        + "pSection");
            }
        }
    }

    /**
     * Factory method to create appropriate parsed section class for the section
     * @param aSection Vector containing the section of script
     * @return a ParesedSection object
     */
    private static ParsedSection parsedSectionFactory(
            HsqlArrayList aSection) {

        //type of the section
        char type = ' ';

        //section represented as an array of Strings, one for each significant
        //line in the section
        String[] rows = null;

        //read the first line of the Vector...
        String topLine = (String) aSection.get(0);

        //...and check it for the type...
        if (topLine.startsWith("/*")) {
            type = topLine.charAt(2);

            //if the type code is UPPERCASE and system property IgnoreCodeCase
            //has been set to true, make the type code lowercase
            if ((Character.isUpperCase(type))
                    && (Boolean.getBoolean("IgnoreCodeCase"))) {
                type = Character.toLowerCase(type);
            }

            //if the type code is invalid return null
            if (!ParsedSection.isValidCode(type)) {
                return null;
            }

            //...strip out the type declaration...
            topLine = topLine.substring(3);
        }

        //if, after stripping out the declaration from topLine, the length of topLine
        //is greater than 0, then keep the rest of the line, as the first row.
        //Otherwise it will be discarded, and the offset (between the array and the vector)
        //set to 1.
        int offset = 0;

        if (topLine.trim().length() > 0) {
            rows    = new String[aSection.size()];
            rows[0] = topLine;
        } else {
            rows   = new String[aSection.size() - 1];
            offset = 1;
        }

        //pull the rest of aSection into the rows array.
        for (int i = (1 - offset); i < rows.length; i++) {
            rows[i] = (String) aSection.get(i + offset);
        }

        //then pass this to the constructor for the ParsedSection class that
        //corresponds to the value of type
        switch (type) {

            case 'u' :
                return new UpdateParsedSection(rows);

            case 's' :
                return new SilentParsedSection(rows);

            case 'w' :
                return new WaitSection(rows);

            case 'p' :
                return new ProceedSection(rows);

            case 'r' :
                return new ResultSetParsedSection(rows);

            case 'c' :
                return new CountParsedSection(rows);

            case 'd' :
                return new DisplaySection(rows);

            case 'e' :
                return new ExceptionParsedSection(rows);

            case ' ' :
                return new BlankParsedSection(rows);

            default :

                //if we arrive here, then we should have a valid code,
                //since we validated it earlier, so return an
                //IgnoreParsedSection object
                return new IgnoreParsedSection(rows, type);
        }
    }

    /**
     * This method should certainly be an instance method.
     *
     * Can't do that until make this entire class OO.
     */
    static public void setAbortOnErr(boolean aoe) {
        abortOnErr = aoe;
    }

    static class TestRuntimeException extends RuntimeException {
        public TestRuntimeException(String s) {
            super(s);
        }
        public TestRuntimeException(Throwable t) {
            super(t);
        }
        public TestRuntimeException(String s, Throwable t) {
            super(s, t);
        }
    }
}

/**
 * Abstract inner class representing a parsed section of script.
 * The specific ParsedSections for each type of test should inherit from this.
 */
abstract class ParsedSection {

    /**
     * Type of this test.
     * @see isValidCase() for allowed values
     */
    protected char type = ' ';

    /** error message for this section */
    String message = null;

    /** contents of the section as an array of Strings, one for each line in the section. */
    protected String[] lines = null;

    /** number of the last row containing results in sectionLines */
    protected int resEndRow = 0;

    /** SQL query to be submitted to the database. */
    protected String sqlString = null;

    /**
     * Constructor when the section's input lines do not need to be parsed
     * into SQL.
     */
    protected ParsedSection() {}

    /**
     * Common constructor functions for this family.
     * @param aLines Array of the script lines containing the section of script.
     * database
     */
    protected ParsedSection(String[] aLines) {

        lines = aLines;

        //read the lines array backwards to get out the SQL String
        //using a StringBuffer for efficency until we've got the whole String
        StringBuffer sqlBuff  = new StringBuffer();
        int          endIndex = 0;
        int          k        = lines.length - 1;

        do {

            //check to see if the row contains the end of the result set
            if ((endIndex = lines[k].indexOf("*/")) != -1) {

                //then this is the end of the result set
                sqlBuff.insert(0, lines[k].substring(endIndex + 2));

                lines[k] = lines[k].substring(0, endIndex);

                if (lines[k].length() == 0) {
                    resEndRow = k - 1;
                } else {
                    resEndRow = k;
                }

                break;
            } else {
                sqlBuff.insert(0, lines[k]);
            }

            k--;
        } while (k >= 0);

        //set sqlString value
        sqlString = sqlBuff.toString();
    }

    /**
     * String representation of this ParsedSection
     * @return String representation of this ParsedSection
     */
    public String toString() {

        StringBuffer b = new StringBuffer();

        b.append("\n******\n");
        b.append("contents of lines array:\n");

        for (int i = 0; i < lines.length; i++) {
            if (lines[i].trim().length() > 0) {
                b.append("line ").append(i).append(": ").append(
                    lines[i]).append("\n");
            }
        }

        b.append("Type: ");
        b.append(getType()).append("\n");
        b.append("SQL: ").append(getSql()).append("\n");
        b.append("results:\n");
        b.append(getResultString());

        //check to see if the message field has been populated
        if (getMessage() != null) {
            b.append("\nmessage:\n");
            b.append(getMessage());
        }

        b.append("\n******\n");

        return b.toString();
    }

    /**
     * returns a String representation of the expected result for the test
     * @return The expected result(s) for the test
     */
    protected abstract String getResultString();

    /**
     *  returns the error message for the section
     *
     * @return message
     */
    protected String getMessage() {
        return message;
    }

    /**
     * returns the type of this section
     * @return type of this section
     */
    protected char getType() {
        return type;
    }

    /**
     * returns the SQL statement for this section
     * @return SQL statement for this section
     */
    protected String getSql() {
        return sqlString;
    }

    /**
     * performs the test contained in the section against the database.
     * @param aStatement Statement object
     * @return true if the result(s) are as expected, otherwise false
     */
    protected boolean test(Statement aStatement) {

        try {
            aStatement.execute(getSql());
        } catch (Exception x) {
            message = x.getMessage();

            return false;
        }

        return true;
    }

    /**
     * Checks that the type code letter is valid
     * @param aCode Lower-cased type code to validate.
     * @return true if the type code is valid, otherwise false.
     */
    protected static boolean isValidCode(char aCode) {

        /* Allowed values for test codes are:
         * (note that UPPERCASE codes, while valid are only processed if the
         * system property IgnoreCodeCase has been set to true)
         *
         * 'u' - update
         * 'c' - count
         * 'e' - exception
         * 'r' - results
         * 'w' - wait
         * 'p' - proceed
         * 's' - silent
         * 'd'       - display   (No reason to use upper-case).
         * ' ' - not a test
         */

        switch (aCode) {

            case ' ' :
            case 'r' :
            case 'e' :
            case 'c' :
            case 'u' :
            case 's' :
            case 'd' :
            case 'w' :
            case 'p' :
                return true;
        }

        return false;
    }
}

/** Represents a ParsedSection for a ResultSet test */
class ResultSetParsedSection extends ParsedSection {

    private String delim = System.getProperty("TestUtilFieldDelimiter", ",");
    private String[] expectedRows = null;

    /**
     * constructs a new instance of ResultSetParsedSection, interpreting
     * the supplied results as one or more lines of delimited field values
     * @param lines String[]
     */
    protected ResultSetParsedSection(String[] lines) {

        super(lines);

        type = 'r';

        //now we'll populate the expectedResults array
        expectedRows = new String[(resEndRow + 1)];

        for (int i = 0; i <= resEndRow; i++) {
            int skip = StringUtil.skipSpaces(lines[i], 0);

            expectedRows[i] = lines[i].substring(skip);
        }
    }

    protected String getResultString() {

        StringBuffer printVal = new StringBuffer();

        for (int i = 0; i < getExpectedRows().length; i++) {
            printVal.append(getExpectedRows()[i]).append("\n");
        }

        return printVal.toString();
    }

    protected boolean test(Statement aStatement) {

        try {
            try {

                //execute the SQL
                aStatement.execute(getSql());
            } catch (SQLException s) {
                throw new Exception(
                    "Expected a ResultSet, but got the error: "
                    + s.getMessage());
            }

            //check that update count != -1
            if (aStatement.getUpdateCount() != -1) {
                throw new Exception(
                    "Expected a ResultSet, but got an update count of "
                    + aStatement.getUpdateCount());
            }

            //iterate over the ResultSet
            ResultSet results = aStatement.getResultSet();
            int       count   = 0;

            while (results.next()) {
                if (count < getExpectedRows().length) {

//                    String[] expectedFields = getExpectedRows()[count].split(delim);
                    String[] expectedFields =
                        StringUtil.split(getExpectedRows()[count], delim);

                    //check that we have the number of columns expected...
                    if (results.getMetaData().getColumnCount()
                            == expectedFields.length) {

                        //...and if so, check that the column values are as expected...
                        int j = 0;

                        for (int i = 0; i < expectedFields.length; i++) {
                            j = i + 1;

                            String actual = results.getString(j);

                            //...including null values...
                            if (actual == null) {    //..then we have a null

                                //...check to see if we were expecting it...
                                if (!expectedFields[i].equalsIgnoreCase(
                                        "NULL")) {
                                    throw new Exception(
                                        "Expected row " + count
                                        + " of the ResultSet to contain:\n"
                                        + getExpectedRows()[count]
                                        + "\nbut field " + j
                                        + " contained NULL");
                                }
                            } else if (!actual.equals(expectedFields[i])) {

                                //then the results are different
                                throw new Exception(
                                    "Expected row " + (count + 1)
                                    + " of the ResultSet to contain:\n"
                                    + getExpectedRows()[count]
                                    + "\nbut field " + j + " contained "
                                    + results.getString(j));
                            }
                        }
                    } else {

                        //we have the wrong number of columns
                        throw new Exception(
                            "Expected the ResultSet to contain "
                            + expectedFields.length
                            + " fields, but it contained "
                            + results.getMetaData().getColumnCount()
                            + " fields.");
                    }
                }

                count++;
            }

            //check that we got as many rows as expected
            if (count != getExpectedRows().length) {

                //we don't have the expected number of rows
                throw new Exception("Expected the ResultSet to contain "
                                    + getExpectedRows().length
                                    + " rows, but it contained " + count
                                    + " rows.");
            }
        } catch (Exception x) {
            message = x.getMessage();

            return false;
        }

        return true;
    }

    private String[] getExpectedRows() {
        return expectedRows;
    }
}

/** Represents a ParsedSection for an update test */
class UpdateParsedSection extends ParsedSection {

    //expected update count
    int countWeWant;

    protected UpdateParsedSection(String[] lines) {

        super(lines);

        type        = 'u';
        countWeWant = Integer.parseInt(lines[0]);
    }

    protected String getResultString() {
        return Integer.toString(getCountWeWant());
    }

    private int getCountWeWant() {
        return countWeWant;
    }

    protected boolean test(Statement aStatement) {

        try {
            try {

                //execute the SQL
                aStatement.execute(getSql());
            } catch (SQLException s) {
                throw new Exception("Expected an update count of "
                                    + getCountWeWant()
                                    + ", but got the error: "
                                    + s.getMessage());
            }

            if (aStatement.getUpdateCount() != getCountWeWant()) {
                throw new Exception("Expected an update count of "
                                    + getCountWeWant()
                                    + ", but got an update count of "
                                    + aStatement.getUpdateCount() + ".");
            }
        } catch (Exception x) {
            message = x.getMessage();

            return false;
        }

        return true;
    }
}

class WaitSection extends ParsedSection {
    /* Would love to have a setting to say whether multi-thread mode,
     * but the static design of TestUtil prevents that.
     * a W command will cause a non-threaded execution to wait forever.
     */
static private String W_SYNTAX_MSG =
    "Syntax of Wait commands:\n"
 + "    /*w 123*/     To Wait 123 milliseconds\n"
 + "    /*w false x*/ Wait until /*p*/ command in another script has executed\n"
 + "    /*w true x*/  Same, but the /*p*/ must not have executed yet"; 

/** Represents a ParsedSection for wait execution */
    long sleepTime = -1;
    Waiter waiter = null;
    boolean enforceSequence = false;

    protected WaitSection(String[] inLines) {
        /* Can't user the super constructor, since it does funny things when
         * constructing the SQL Buffer, which we don't need. */
        lines = inLines;
        int closeCmd = lines[0].indexOf("*/");
        String cmd = lines[0].substring(0, closeCmd);
        lines[0] = lines[0].substring(closeCmd + 2).trim();
        String trimmed = cmd.trim();
        if (trimmed.indexOf('e') < 0 && trimmed.indexOf('E') < 0) {
            // Does not contain "true" or "false"
            sleepTime = Long.parseLong(trimmed);
        } else try {
            // Would like to use String.split(), but don't know if Java 4
            // is allowed here.
            // Until we can use Java 4, prohibit tabs as white space.
            int index = trimmed.indexOf(' ');
            if (index < 0) throw new IllegalArgumentException();
            enforceSequence =
                    Boolean.valueOf(trimmed.substring(0, index)).booleanValue();
            waiter = Waiter.getWaiter(trimmed.substring(index).trim());
        } catch (IllegalArgumentException ie) {
            throw new IllegalArgumentException(W_SYNTAX_MSG);
        }

        type = 'w';
    }

    protected String getResultString() {
        StringBuffer sb = new StringBuffer();
        if (lines.length == 1 && lines[0].trim().length() < 1) {
            return null;
        }

        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                sb.append('\n');
            }
            sb.append("+ " + lines[i]);
        }
        TestUtil.expandStamps(sb);
        return sb.toString().trim();
    }

    protected boolean test(Statement aStatement) {
        if (waiter == null) try {
            //System.err.println("Sleeping for " + sleepTime + " ms.");
            Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
            throw new RuntimeException("Test sleep interrupted", ie);
        } else {
            waiter.waitFor(enforceSequence);
        }
        return true;
    }
}

class ProceedSection extends ParsedSection {
    /* See comment above for WaitSection */
static private String P_SYNTAX_MSG =
    "Syntax of Proceed commands:\n"
 + "    /*p false x*/ /*p*/ command in another script may Proceed\n"
 + "    /*p true x*/  Same, but the /*w*/ must be waiting when we execute /*p*/"
;

/** Represents a ParsedSection for wait execution */
    Waiter waiter = null;
    boolean enforceSequence = false;

    protected ProceedSection(String[] inLines) {
        /* Can't user the super constructor, since it does funny things when
         * constructing the SQL Buffer, which we don't need. */
        lines = inLines;
        int closeCmd = lines[0].indexOf("*/");
        String cmd = lines[0].substring(0, closeCmd);
        lines[0] = lines[0].substring(closeCmd + 2).trim();
        String trimmed = cmd.trim();
        try {
            // Would like to use String.split(), but don't know if Java 4
            // is allowed here.
            // Until we can use Java 4, prohibit tabs as white space.
            int index = trimmed.indexOf(' ');
            if (index < 0) throw new IllegalArgumentException();
            enforceSequence =
                    Boolean.valueOf(trimmed.substring(0, index)).booleanValue();
            waiter = Waiter.getWaiter(trimmed.substring(index).trim());
        } catch (IllegalArgumentException ie) {
            throw new IllegalArgumentException(P_SYNTAX_MSG);
        }

        type = 'p';
    }

    protected String getResultString() {
        StringBuffer sb = new StringBuffer();
        if (lines.length == 1 && lines[0].trim().length() < 1) {
            return null;
        }

        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                sb.append('\n');
            }
            sb.append("+ " + lines[i]);
        }
        TestUtil.expandStamps(sb);
        return sb.toString().trim();
    }

    protected boolean test(Statement aStatement) {
        waiter.resume(enforceSequence);
        return true;
    }
}

/** Represents a ParsedSection for silent execution */
class SilentParsedSection extends ParsedSection {

    protected SilentParsedSection(String[] lines) {

        super(lines);

        type = 's';
    }

    protected String getResultString() {
        return null;
    }

    protected boolean test(Statement aStatement) {

        try {
            aStatement.execute(getSql());
        } catch (Exception x) {}

        return true;
    }
}

/** Represents a ParsedSection for a count test */
class CountParsedSection extends ParsedSection {

    //expected row count
    private int countWeWant;

    protected CountParsedSection(String[] lines) {

        super(lines);

        type        = 'c';
        countWeWant = Integer.parseInt(lines[0]);
    }

    protected String getResultString() {
        return Integer.toString(getCountWeWant());
    }

    private int getCountWeWant() {
        return countWeWant;
    }

    protected boolean test(Statement aStatement) {

        try {

            //execute the SQL
            try {
                aStatement.execute(getSql());
            } catch (SQLException s) {
                throw new Exception("Expected a ResultSet containing "
                                    + getCountWeWant()
                                    + " rows, but got the error: "
                                    + s.getMessage());
            }

            //check that update count != -1
            if (aStatement.getUpdateCount() != -1) {
                throw new Exception(
                    "Expected a ResultSet, but got an update count of "
                    + aStatement.getUpdateCount());
            }

            //iterate over the ResultSet
            ResultSet results = aStatement.getResultSet();
            int       count   = 0;

            while (results.next()) {
                count++;
            }

            //check that we got as many rows as expected
            if (count != getCountWeWant()) {

                //we don't have the expected number of rows
                throw new Exception("Expected the ResultSet to contain "
                                    + getCountWeWant()
                                    + " rows, but it contained " + count
                                    + " rows.");
            }
        } catch (Exception x) {
            message = x.getMessage();

            return false;
        }

        return true;
    }
}

/** Represents a ParsedSection for an Exception test */
class ExceptionParsedSection extends ParsedSection {
    private String expectedState = null;
    private Throwable caught = null;

    protected ExceptionParsedSection(String[] lines) {

        super(lines);
        expectedState = lines[0].trim();
        if (expectedState.length() < 1) {
            expectedState = null;
        }

        type = 'e';
    }

    protected String getResultString() {
        return (caught == null) ? "Nothing thrown" : caught.toString();
    }

    protected boolean test(Statement aStatement) {

        try {
            aStatement.execute(getSql());
        } catch (SQLException sqlX) {
            caught = sqlX;
            if (expectedState == null
                    || expectedState.equalsIgnoreCase(sqlX.getSQLState())) {
                return true;
            }
            message = "SQLState '" + sqlX.getSQLState() + "' instead of '"
                + expectedState + "': " + sqlX.getMessage();
        } catch (Exception x) {
            caught = x;
            message = x.getMessage();
        }

        return false;
    }
}

/** Represents a ParsedSection for a section with blank type */
class BlankParsedSection extends ParsedSection {

    protected BlankParsedSection(String[] lines) {

        super(lines);

        type = ' ';
    }

    protected String getResultString() {
        return "No result specified for this section";
    }
}

/** Represents a ParsedSection that is to be ignored */
class IgnoreParsedSection extends ParsedSection {

    protected IgnoreParsedSection(String[] inLines, char aType) {

        /* Extremely ambiguous to use input parameter of same exact
         * variable name as the superclass member "lines".
         * Therefore, renaming to inLines. */

        // Inefficient to parse this into SQL when we aren't going to use
        // it as SQL.  Should probably just be removed to use the 
        // super() constructor.
        super(inLines);

        type = aType;
    }

    protected String getResultString() {
        return "This section, of type '" + getType() + "' was ignored";
    }
}

/** Represents a Section to be Displayed, not executed */
class DisplaySection extends ParsedSection {

    protected DisplaySection(String[] inLines) {

        /* Can't user the super constructor, since it does funny things when
         * constructing the SQL Buffer, which we don't need. */
        lines = inLines;

        int firstSlash = lines[0].indexOf('/');

        lines[0] = lines[0].substring(firstSlash + 1).trim();
    }

    protected String getResultString() {
        StringBuffer sb = new StringBuffer();
        if (lines.length == 1 && lines[0].trim().length() < 1) {
            return null;
        }

        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                sb.append('\n');
            }
            sb.append("+ " + lines[i]);
        }
        TestUtil.expandStamps(sb);
        return sb.toString().trim();
    }
}
