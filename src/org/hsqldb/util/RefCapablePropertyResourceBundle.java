/* Copyright (c) 2001-2007, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.util;

import java.util.PropertyResourceBundle;
import java.util.Map;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.MissingResourceException;
import java.io.InputStream;
import java.io.IOException;

/**
 * Just like PropertyResourceBundle, except keys mapped to nothing in the
 * properties file will load the final String value from a text file.
 *
 * The use case is where one wants to use a ResourceBundle for Strings,
 * but some of the Strings are long-- too long to maintain in a Java
 * .properties file.
 * By using this class, you can put each such long String in its own
 * separate file, yet all keys mapped to (non-empty) values in the
 * .properties file will behave just like regular PropertyResourceBundle
 * properties.
 * In this documentation, I call these values read in atomically from
 * other files <i>referenced</i> values, because the values are not directly
 * in the .properties file, but are "referenced" in the .properties file
 * by virtue of the empty value for the key.
 *
 * You use this class in the same way as you would traditionally use
 * ResourceBundle:
 * <PRE>
 *  import org.hsqldb.util..RefCapablePropertyResourceBundle;
 *  ...
 *      RefCapablePropertyResourceBundle bundle =
 *              RefCapablePropertyResourceBundle.getBundle("subdir.xyz");
 *      System.out.println("Value for '1' = (" + bundle.getString("1") + ')');
 * </PRE>
 *
 * Just like PropertyResourceBundle, the .properties file and the 
 * <i>referenced</i> files are read in from the classpath by a class loader,
 * according to the normal ResourceBundle rules.
 * To eliminate the need to prohibit the use of any strings in the .properties
 * values, and to enforce consistency, you <b>must</b> use the following rules
 * to when putting your referenced files into place.
 * <P/>
 * REFERENCED FILE DIRECTORY is a directory named with the base name of the
 * properties file, and in the same parent directory.  So, the referenced
 * file directory <CODE>/a/b/c/greentea</CODE> is used to hold all reference 
 * files for properties files <CODE>/a/b/c/greentea_en_us.properties</CODE>, 
 * <CODE>/a/b/c/greentea_de.properties</CODE>,
 * <CODE>/a/b/c/greentea.properties</CODE>, etc.
 * (BTW, according to ResourceBundle rules, this resource should be looked
 * up with name "a.b.c.greentea", not "/a/b/c..." or "a/b/c").
 * REFERENCED FILES themselves all have the base name of the property key,
 * with locale appendages exactly as the <i>referring</i> properties files
 * has, plus the suffix <CODE>.text</CODE>.
 * <P/>
 * So, if we have the following line in 
 * <CODE>/a/b/c/greentea_de.properties</CODE>:
 * <PRE>
 *     1: eins
 * </PRE>
 * then you <b>must</b> have a reference text file
 * <CODE>/a/b/c/greentea/1_de.properties</CODE>:
 * <P/>
 * In reference text files,
 * sequences of "\r", "\n" and "\r\n" are all translated to the line
 * delimiter for your platform (System property <CODE>line.separator</CODE>).
 * If one of those sequences exists at the very end of the file, it will be
 * eliminated.
 * (The file itself is never modified-- I'm talking about the value returned
 * by <CODE>getString(String)</CODE>.
 *
 * @see java.util.PropertyResourceBundle
 * @see java.util.ResourceBundle
 * @author  blaine.simpson@admc.com
 */
public class RefCapablePropertyResourceBundle {
    private PropertyResourceBundle wrappedBundle;
    private String baseName;
    private String language, country, variant;
    static private Map allBundles = new HashMap();
    public static String LS = System.getProperty("line.separator");

    private RefCapablePropertyResourceBundle(String baseName,
            PropertyResourceBundle wrappedBundle) {
        this.baseName = baseName;
        this.wrappedBundle = wrappedBundle;
        Locale locale = wrappedBundle.getLocale();
        language = locale.getLanguage();
        country = locale.getCountry();
        variant = locale.getVariant();
        if (language.length() < 1) language = null;
        if (country.length() < 1) country = null;
        if (variant.length() < 1) variant = null;
    }

    /**
     * Just identifies this RefCapablePropertyResourceBundle instance.
     */
    public String toString() {
        return baseName + " for " + language + " / " + country + " / "
            + variant;
    }

    /**
     * Returns value defined in this RefCapablePropertyResourceBundle's
     * .properties file, unless that value is empty.
     * If the value in the .properties file is empty, then this returns
     * the entire contents of the referenced text file.
     *
     * @see ResourceBundle#get(String)
     */
    public String getString(String key) {
        String value = wrappedBundle.getString(key);
        if (value.length() > 0) return value;
        value = getStringFromFile(key);
        // For conciseness and sanity, get rid of all \r's so that \n
        // will definitively be our line breaks.
        if (value.indexOf('\r') > -1)
            value = value.replaceAll("\\r\\n", "\n").replaceAll("\\r", "\n");
        if (value.length() > 0 && value.charAt(value.length() - 1) == '\n')
            value = value.substring(0, value.length() - 1);
        if (!LS.equals("\n")) value = value.replaceAll("\\n", LS);
        return value;
    }

    /**
     * Use exactly like java.util.ResourceBundle.get(String).
     *
     * @see ResourceBundle#get(String)
     */
    public static RefCapablePropertyResourceBundle getBundle(String baseName) {
        return getRef(baseName, ResourceBundle.getBundle(baseName));
    }
    /**
     * Use exactly like java.util.ResourceBundle.get(String, Locale).
     *
     * @see ResourceBundle#get(String, Locale)
     */
    public static RefCapablePropertyResourceBundle
            getBundle(String baseName, Locale locale) {
        return getRef(baseName, ResourceBundle.getBundle(baseName, locale));
    }
    /**
     * Use exactly like java.util.ResourceBundle.get(String, Locale, ClassLoader).
     *
     * @see ResourceBundle#get(String, Locale, ClassLoader)
     */
    public static RefCapablePropertyResourceBundle
            getBundle(String baseName, Locale locale, ClassLoader loader) {
        return getRef(baseName,
                ResourceBundle.getBundle(baseName, locale, loader));
    }

    /**
     * Return a ref to a new or existing RefCapablePropertyResourceBundle,
     * or throw a MissingResourceException.
     */
    static private RefCapablePropertyResourceBundle getRef(String baseName,
            ResourceBundle rb) {
        if (!(rb instanceof PropertyResourceBundle))
            throw new MissingResourceException(
                    "Found a Resource Bundle, but it is a "
                            + rb.getClass().getName(),
                    PropertyResourceBundle.class.getName(), null);
        if (allBundles.containsKey(rb))
            return (RefCapablePropertyResourceBundle) allBundles.get(rb);
        RefCapablePropertyResourceBundle newPRAFP =
                new RefCapablePropertyResourceBundle(baseName,
                        (PropertyResourceBundle) rb);
        allBundles.put(rb, newPRAFP);
        return newPRAFP;
    }

    private String getStringFromFile(String key) {
        String filePath = "/" + baseName.replace('.', '/') + '/' + key
                + ((language == null) ? "" : ("_" + language))
                + ((country == null) ? "" : ("_" + country))
                + ((variant == null) ? "" : ("_" + variant))
                + ".text";
        InputStream  inputStream = getClass().getResourceAsStream(filePath);
        if (inputStream == null)
            throw new MissingResourceException(
                    "Key is present in .properties file with no value, yet "
                    + "resource '" + filePath + "' is missing",
                    RefCapablePropertyResourceBundle.class.getName(), key);
        byte[] ba = null;
        try {
            ba = new byte[inputStream.available()];
            if (inputStream.read(ba) != ba.length)
                throw new MissingResourceException(
                        "Resource '" + filePath + "' changed while reading",
                        RefCapablePropertyResourceBundle.class.getName(), key);
        } catch (IOException ioe) {
            throw new MissingResourceException(
                    "Failed to allocate a byte buffer: " + ioe,
                    RefCapablePropertyResourceBundle.class.getName(), key);
        }
        return new String(ba);
    }
}
