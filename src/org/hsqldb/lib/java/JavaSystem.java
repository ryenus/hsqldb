/* Copyright (c) 2001-2022, The HSQL Development Group
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
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.charset.Charset;

/**
 * Handles invariants, runtime and methods
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.6.1
 */
public final class JavaSystem {

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
            } else if (version.startsWith("0.")) {
                version = "6";
            }

            javaVersion = Integer.parseInt(version);
        } catch (Throwable t) {

            // unknow future version - default to last widely used
            javaVersion = 11;
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

                return null;
            } catch (NoSuchMethodException e) {}
            catch (NoSuchFieldException e) {}
            catch (IllegalAccessException e) {}
            catch (ClassNotFoundException e) {}
            catch (InvocationTargetException e) {
                return e;
            } catch (Throwable t) {
                return t;
            }

            // on any reflection error we assume that we made a mistake guessing the java version
            // and try the old code instead
        }

        try {
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");

            cleanerMethod.setAccessible(true);

            Object cleaner     = cleanerMethod.invoke(buffer);
            Method cleanMethod = cleaner.getClass().getMethod("clean");

            cleanMethod.invoke(cleaner);
            return null;
        } catch (NoSuchMethodException e) {}
        catch (IllegalAccessException e) {}
        catch (InvocationTargetException e) {

            // means we're not dealing with a Sun JVM?
            return e;
        } catch (Throwable t) {
            return t;
        }

        // try another fallback on any reflection error
        // this is specific to older Android version 4 or older and works for java.nio.DirectByteBuffer and java.nio.MappedByteBufferAdapter
        try {
            Method freeMethod = buffer.getClass().getMethod("free");

            freeMethod.setAccessible(true);

            freeMethod.invoke(buffer);
            return null;
        } catch (Throwable t) {
            return t;
        }
    }

    public static IOException toIOException(Throwable t) {

        if (t instanceof IOException) {
            return (IOException) t;
        }

        return new IOException(t);
    }
}
