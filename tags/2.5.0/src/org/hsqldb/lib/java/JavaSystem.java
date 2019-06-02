/* Copyright (c) 2001-2019, The HSQL Development Group
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


package org.hsqldb.lib.java;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.DriverManager;
import java.nio.charset.Charset;
import java.nio.MappedByteBuffer;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Handles invariants, runtime and methods
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.5.0
 */
public class JavaSystem {

    public static final Charset CS_ISO_8859_1 = Charset.forName("ISO-8859-1");
    public static final Charset CS_US_ASCII   = Charset.forName("US-ASCII");
    public static final Charset CS_UTF8       = Charset.forName("UTF-8");
    private static int          javaVersion;

    static {
        try {
            String version = System.getProperty("java.specification.version",
                                                "6");

            if (version.startsWith("1.")) {
                version = version.substring(2);
            }

            javaVersion = Integer.parseInt(version);
        } catch (Throwable t) {

            // unknow future version - default to last known
            javaVersion = 12;
        }
    }

    public static int javaVersion() {
        return javaVersion;
    }

    // Memory
    public static long availableMemory() {
        return Runtime.getRuntime().freeMemory();
    }

    public static long usedMemory() {
        return Runtime.getRuntime().totalMemory()
               - Runtime.getRuntime().freeMemory();
    }

    public static Throwable unmap(MappedByteBuffer buffer) {

        if (buffer == null) {
            return null;
        }

        if (javaVersion > 8) {
            try {
                Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
                Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");

                unsafeField.setAccessible(true);

                Object unsafe = unsafeField.get(null);
                Method invokeCleaner = unsafeClass.getMethod("invokeCleaner",
                    java.nio.ByteBuffer.class);

                invokeCleaner.invoke(unsafe, buffer);
            } catch (Throwable t) {
                return t;
            }
        } else {
            try {
                Method cleanerMethod = buffer.getClass().getMethod("cleaner");

                cleanerMethod.setAccessible(true);

                Object cleaner     = cleanerMethod.invoke(buffer);
                Method cleanMethod = cleaner.getClass().getMethod("clean");

                cleanMethod.invoke(cleaner);
            } catch (NoSuchMethodException e) {
                // no cleaner
                return e;
            } catch (InvocationTargetException e) {
                // means we're not dealing with a Sun JVM?
                return e;
            } catch (Throwable t) {
                return t;
            }
        }

        return null;
    }

    public static IOException toIOException(Throwable t) {

        if (t instanceof IOException) {
            return (IOException) t;
        }

        return new IOException(t);
    }

    static final BigDecimal BD_1  = BigDecimal.valueOf(1L);
    static final BigDecimal MBD_1 = BigDecimal.valueOf(-1L);

    public static int precision(BigDecimal o) {

        if (o == null) {
            return 0;
        }

        int precision;

        if (o.compareTo(BD_1) < 0 && o.compareTo(MBD_1) > 0) {
            precision = o.scale();
        } else {
            precision = o.precision();
        }

        return precision;
    }
}
