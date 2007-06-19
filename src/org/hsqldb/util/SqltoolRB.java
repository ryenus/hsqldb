package org.hsqldb.util;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Enumeration;

/**
 * Resource Bundle for SqlTool and associated classes.
 *
 * This wraps a static ResourceBundle famaily.  For that reason this class
 * is entirely static.
 *
 * Purpose of this class is to wrap a RefCapablePropertyResourceBundle to
 *  reliably detect any possible use of a missing property key as soon as
 *  this class is clinitted.
 * The reason for this is to allow us developers to detect all such errors
 *  before end-users ever use this class.
 *
 * IMPORTANT:  To add a new ResourceBundle element, add two new lines, one
 * like <PRE>
 *    static public final int NEWKEYID = keyCounter++;
 * </PRE> and one line <PRE>
 *      new Integer(KEY2), "key2",
 * </PRE>
 * Both should be inserted right after all of the other lines of the same type.
 * NEWKEYID is obviously a new constant which you will use in calling code
 * like SqltoolRB.NEWKEYID.
 */
public class SqltoolRB {
    static public void main(String[] sa) {
        SqltoolRB.validate();
        SqltoolRB.setMissingSubstValueBehavior(
             RefCapablePropertyResourceBundle.NOOP_BEHAVIOR);
        SqltoolRB.setMissingPropertyBehavior(
             RefCapablePropertyResourceBundle.EMPTYSTRING_BEHAVIOR);
        System.err.println("key2 -> ("
                + SqltoolRB.getExpandedString(SqltoolRB.KEY2,
                        new String[] { "one", "two" }
                ) + ')');
    }

    static private int keyCounter = 0;
    static public final int KEY0 = keyCounter++;
    static public final int KEY1 = keyCounter++;
    static public final int KEY2 = keyCounter++;
    static private boolean validated = false;

    private static Object[] memberKeyArray = new Object[] {
        new Integer(KEY0), "key0",
        new Integer(KEY1), "key1",
        new Integer(KEY2), "key2",
    };


    static private RefCapablePropertyResourceBundle wrappedRCPRB =
        RefCapablePropertyResourceBundle.getBundle("org.hsqldb.util.sqltool");

    static private Map keyIdToString = new HashMap();
    static {
        if (memberKeyArray == null)
            throw new RuntimeException("'memberKeyArray not overridden");
        Integer iger;
        String s;
        for (int i = 0; i < memberKeyArray.length; i += 2) {
            if (!(memberKeyArray[i] instanceof Integer))
                throw new RuntimeException("Element #" +  i
                        + " of memberKeyArray is not an Integer:  "
                        + memberKeyArray[i].getClass().getName());
            if (!(memberKeyArray[i+1] instanceof String))
                throw new RuntimeException("Element #" +  (i+1)
                        + " of memberKeyArray is not an Integer:  "
                        + memberKeyArray[i+1].getClass().getName());
            if (((Integer) memberKeyArray[i]).intValue() != i/2)
                throw new RuntimeException(
                        "Wrong contstant before element \""
                        + memberKeyArray[i+1] + "\" in array "
                        + "memberKeyArray in class "
                        + SqltoolRB.class.getName());
            keyIdToString.put(memberKeyArray[i], memberKeyArray[i+1]);
        }
        System.err.println("Initialized keyIdToString map with "
                + keyIdToString.size() + " mappings");
    }

    public static void validate() {
        if (validated) return;
        validated = true;
        Set allIdStrings = new HashSet(keyIdToString.values());
        Enumeration allKeys = wrappedRCPRB.getKeys();
        while (allKeys.hasMoreElements())
            allIdStrings.remove(allKeys.nextElement());
        if (allIdStrings.size() > 0)
            throw new RuntimeException(
                    "Resource Bundle pre-validation failed.  "
                    + "Following property key(s) not mapped.\n" + allIdStrings);
    }

    // The following methods are a passthru wrappers for the wrapped RCPRB.

    /** @see RefCapablePropertyResourceBundle#getString(String) */
    public static String getString(int id) {
        return wrappedRCPRB.getString((String) keyIdToString.get(id));
    }

    /** @see RefCapablePropertyResourceBundle#getString(String, String[]) */
    public static String getString(int id, String[] sa) {
        return wrappedRCPRB.getString((String) keyIdToString.get(id), sa);
    }

    /** @see RefCapablePropertyResourceBundle#getExpandedString(String) */
    public static String getExpandedString(int id) {
        return wrappedRCPRB.getExpandedString((String) keyIdToString.get(id));
    }

    /** @see RefCapablePropertyResourceBundle#getExpandedString(String, String[]) */
    public static String getExpandedString(int id, String[] sa) {
        return wrappedRCPRB.getExpandedString(
                (String) keyIdToString.get(id), sa);
    }

    /** @see RefCapablePropertyResourceBundle#setMissingPropertyBehavior(int) */
    public static void setMissingPropertyBehavior(int missingPropertyBehavior) {
        wrappedRCPRB.setMissingPropertyBehavior(missingPropertyBehavior);
    }

    /** @see RefCapablePropertyResourceBundle#setMissingSubstValueBehavior(int) */
    public static void setMissingSubstValueBehavior(
            int missingSubstValueBehavior) {
        wrappedRCPRB.setMissingSubstValueBehavior(missingSubstValueBehavior);
    }

    /** @see RefCapablePropertyResourceBundle#getMissingPropertyBehavior() */
    public static int getMissingPropertyBehavior() {
        return wrappedRCPRB.getMissingPropertyBehavior();
    }

    /** @see RefCapablePropertyResourceBundle#getMissingSubstValueBehavior() */
    public static int getMissingSubstValueBehavior() {
        return wrappedRCPRB.getMissingSubstValueBehavior();
    }
}
