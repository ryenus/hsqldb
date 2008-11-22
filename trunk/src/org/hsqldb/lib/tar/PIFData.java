package org.hsqldb.lib.tar;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Pax Interchange Format object constituted from an Input Stream.
 * <P/>
 * Right now, the only Pax property that we support directly is "size".
 */
public class PIFData extends HashMap {
    private static Pattern pifRecordPattern =
            Pattern.compile("\\d+ +(.+)=(.+)");

    /**
     * n.b. this is nothing to do with HashMap.size() or Map.size().
     * This returns the value of the Pax "size" property.
     */
    public Long getSize() {
        return sizeObject;
    }

    private Long sizeObject = null;

    public PIFData(InputStream stream)
            throws TarMalformatException, IOException {
        try {
            BufferedReader br =
                    new BufferedReader(new InputStreamReader(stream, "UTF-8"));
            String s;
            Matcher m;
            int eqAt;
            int lineNum = 0;
            while ((s = br.readLine() ) != null) {
                lineNum++;
                s = s.trim();
                if (s.length() < 1 || s.charAt(0) == '#' ) {
                    // I don't know if Pax data format allows these comments,
                    // and white space, but it should.
                    continue;
                }
                m = pifRecordPattern.matcher(s);
                if (!m.matches()) {
                    throw new TarMalformatException(
                            "Line " + lineNum
                            + " of PIF Data is malformatted:\n" + s);
                } 
                put(m.group(1), m.group(2));
            }
        } finally {
            stream.close();
        }

        String sizeString = (String) get("size");
        if (sizeString != null) {
            try {
                sizeObject = Long.parseLong(sizeString);
            } catch (NumberFormatException nfe) {
                throw new TarMalformatException(
                        "PIF Data contains malformatted 'size' value:  "
                        + sizeString);
            }
        }
    }
}
