/* Copyright (c) 2001-2011, The HSQL Development Group
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

package org.hsqldb.testbase;

import org.hsqldb.resources.ResourceBundleHandler;

/**
 * Centralized facility for retrieving test suite properties. <p>
 *
 * Uses the following lookup precedence:<p>
 *
 * <ol>
 *   <li>(System Property) java.lang.System.getProperty(...)
 *   <li>(ResourceBundle) CLASSPATH:/org/hsqldb/resources/test(locale_name).class
 *   <li>(Properties file) CLASSPATH:/org/hsqldb/resources/test(locale_name).properties
 *   <li>(ResourceBundle) CLASSPATH:/org/hsqldb/resources/test-dbmd-convert(locale_name).class
 *   <li>(Properties file) CLASSPATH:/org/hsqldb/resources/test-dbmd-convert(locale_name).properties
 * </ol>
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 2.0.1
 * @since 2.0.1
 */
public class PropertyGetter {
    public static final int BH_TEST = ResourceBundleHandler.getBundleHandle("test", null);
    public static final int BH_TEST_DBMD_CONVERT = ResourceBundleHandler.getBundleHandle("test-dbmd-convert", null);
    
    /**
     * for the given key.
     *
     * @param key to match.
     * @param defaultValue when there is no matching property.
     * @return the matching property value or the default.
     */
    public static String getProperty(final String key, final String defaultValue) {
        String value = null;
        // Note: some properties may be submitted on the command line and
        // should override property files / resource bundle classes on the
        // class path.
        try {
            value = System.getProperty(key, null);
        } catch (SecurityException se) {
        }
        if (value == null) {
            value = ResourceBundleHandler.getString(BH_TEST, key);
        }
        if (value == null) {
            value = ResourceBundleHandler.getString(BH_TEST_DBMD_CONVERT, key);
        }
        if (value == null) {
            value = defaultValue;
        }
        return value;
    } 
    
    public static boolean getBooleanProperty(final String key,
            final boolean defaultValue) {
        try {
            String value = PropertyGetter.getProperty(key, String.valueOf(defaultValue));
            if (value.equalsIgnoreCase("true")
                    || value.equalsIgnoreCase("on")
                    || value.equals("1")) {
                return true;
            } else if (value.equalsIgnoreCase("false")
                    || value.equalsIgnoreCase("off")
                    || value.equals("0")) {
                return false;
            } else {
                return defaultValue;
            }
        } catch (SecurityException se) {
            return defaultValue;
        }
    }    
    
    public static int getIntProperty(final String key, final int defaultValue) {
        String propertyValue = PropertyGetter.getProperty(key, null);
        int rval = defaultValue;

        if (propertyValue != null) {
            propertyValue = propertyValue.trim();

            if (propertyValue.length() > 0) {
                try {
                    rval = Integer.parseInt(propertyValue);
                } catch (Exception ex) {
                }
            }
        }

        return rval;
    }
    
    public static double getDoubleProperty(final String key, final double defaultValue) {
        String propertyValue = PropertyGetter.getProperty(key, null);
        double rval = defaultValue;

        if (propertyValue != null) {
            propertyValue = propertyValue.trim();

            if (propertyValue.length() > 0) {
                try {
                    rval = Double.parseDouble(propertyValue);
                } catch (Exception ex) {
                }
            }
        }

        return rval;
    }    

    /**
     * Construction disabled - pure utility class.
     */
    private PropertyGetter() {
    }
}
