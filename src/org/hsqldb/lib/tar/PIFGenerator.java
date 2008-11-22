package org.hsqldb.lib.tar;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.File;

/**
 * Encapsulates Pax Interchange Format key/value pairs.
 */
public class PIFGenerator extends ByteArrayOutputStream {
    OutputStreamWriter writer;
    String name;
    int fakePid;  // Only used by contructors
    char typeFlag;

    public String getName() {
        return name;
    }

    protected PIFGenerator() {
        try {
            writer = new OutputStreamWriter(this, "UTF-8");
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException(
                    "Serious problem.  JVM can't encode UTF-8", uee);
        }
        fakePid = (int) new java.util.Date().getTime() % 100000;
        // Java doesn't have access to PIDs, as PIF wants in the "name" field,
        // so we emulate one in a way that is easy for us.
    }

    /**
     * Construct a PIFGenerator object for a 'g' record.
     *
     * @param sequenceNum  Index starts at 1 in each Tar file
     */
    public PIFGenerator(int sequenceNum) {
        this();
        typeFlag = 'g';
        name = System.getProperty("java.io.tmpdir") + "/GlobalHead." + fakePid
                     + '.' + sequenceNum;
    }

    /**
     * Construct a PIFGenerator object for a 'x' record.
     *
     * @param file Target file of the x record.
     */
    public PIFGenerator(File file) {
        this();
        typeFlag = 'x';
        String parentPath = (file.getParentFile() == null)
                          ? "."
                          : file.getParentFile().getPath();
        name = parentPath + "PaxHeaders." + fakePid + '/' + file.getName();
    }

    /**
     * I guess the "initial length" field is supposed to be in units of
     * characters, not bytes?
     */
    public void addRecord(String key, String value)
            throws TarMalformatException, IOException {
        if (key == null || value == null
                || key.length() < 1 || value.length() < 1) {
            throw new TarMalformatException(
                    "Refusing to write record with zero-length key or value");
        }
        int lenWithoutIlen = key.length() + value.length() + 3;
        // "Ilen" means Initial Length field.  +3 = SPACE + = + \n
        int lenW = 0; // lenW = Length With initial-length-field
        if (lenWithoutIlen < 8) {
            lenW = lenWithoutIlen + 1; // Takes just 1 char to report total
        } else if (lenWithoutIlen < 97) {
            lenW = lenWithoutIlen + 2; // Takes 2 chars to report this total
        } else if (lenWithoutIlen < 996) {
            lenW = lenWithoutIlen + 3; // Takes 3...
        } else if (lenWithoutIlen < 9995) {
            lenW = lenWithoutIlen + 4; // ditto
        } else if (lenWithoutIlen < 99994) {
            lenW = lenWithoutIlen + 5;
        } else {
            throw new TarMalformatException("Total key + vale lengths exceeds "
                    + "our total supported max of " + 99991);
        }
        /*
         * TODO:  Remove this Dev assertion:
         */
        if (lenW != (Integer.toString(lenW) + ' ' + key + '=' + value + '\n')
                .length()) throw new RuntimeException("ASSERTION FAILED");

        writer.write(Integer.toString(lenW));
        writer.write(' ');
        writer.write(key);
        writer.write('=');
        writer.write(value);
        writer.write('\n');
        writer.flush();  // Does this do anything with a BAOS?
    }

    /**
     * This is a Unit Test.  Move it to a proper, dedicated unit test class.
     */
    static public void main(String[] sa)
            throws TarMalformatException, IOException {
        if (sa.length > 1)  {
            throw new IllegalArgumentException(
                    "java " + PIFGenerator.class.getName() + " [xTargetPath]");
        }
        PIFGenerator pif = (sa.length < 1)
                         ? (new PIFGenerator(1))
                         : (new PIFGenerator(new File(sa[0])));
        pif.addRecord("o", "n");  // Shortest possible
        pif.addRecord("k1", "23");  // total 8.  Impossible to get total of 9.
        pif.addRecord("k2", "234");  // total 10
        pif.addRecord("k3", "2345");  // total 11
        pif.addRecord("k4", "2345678901234567890123456789012345678901234567890"
                + "123456789012345678901234567890123456789012"); //total 98
        // Impossible to get total of 99.
        pif.addRecord("k5", "2345678901234567890123456789012345678901234567890"
                + "1234567890123456789012345678901234567890123");//total 100
        System.out.write(pif.toByteArray());
    }
}
