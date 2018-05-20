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
package org.hsqldb.map;

import java.util.Random;

import org.hsqldb.lib.StopWatch;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;
import junit.framework.Test;
import junit.framework.TestSuite;

@ForSubject(ValuePoolHashMap.class)
public class ValuePoolHashMapTest extends BaseTestCase {

    @OfMethod("getOrAddInteger(int)")
    public void testValuePoolHashMap() {

        int BIGRANGE = 100000;
        int SMALLRANGE = 50000;
        int POOLSIZE = 1000;
        Random randomgen = new java.util.Random();
        StopWatch sw = new org.hsqldb.lib.StopWatch();
        ValuePoolHashMap map = new ValuePoolHashMap(POOLSIZE, POOLSIZE,
                BaseHashMap.PURGE_HALF);
        int maxCount = 5000000;

        try {
            for (int rounds = 0; rounds < 3; rounds++) {
                sw.zero();

                // timing for ValuePool retreival
                for (int i = 0; i < maxCount; i++) {
                    boolean bigrange = (i % 2) == 0;
                    int intValue = randomgen.nextInt(bigrange ? BIGRANGE
                            : SMALLRANGE);
                    Integer intObject = map.getOrAddInteger(intValue);

                    if (intObject.intValue() != intValue) {
                        throw new Exception("Value mismatch");
                    }
                }

                System.out.println("Count " + maxCount + " " + sw.elapsedTime());
                sw.zero();

                // timing for Integer creation
                for (int i = 0; i < maxCount; i++) {
                    boolean bigrange = (i % 2) == 0;
                    int intValue = randomgen.nextInt(bigrange ? BIGRANGE
                            : SMALLRANGE);
                    Integer intObject = new Integer(intValue);

                    if (intObject.intValue() != intValue) {
                        throw new Exception("Value mismatch");
                    }
                }

                System.out.println("Count new Integer() " + maxCount + " " + sw.elapsedTime());
            }
        } catch (Exception e) {
            printException(e);
        }
    }

    static int poolFactor = 8;
    static int sampleFactor = 32;

    @ForSubject(ValuePoolHashMap.class)
    public void testValuePoolIntegerHashMap() {
        ValuePoolHashMap pool   = new ValuePoolHashMap(1024 * poolFactor, 1024 * poolFactor, BaseHashMap.PURGE_HALF);
        Random             random = new Random();

        long millis = System.currentTimeMillis();
        for (long i = 0; i < Integer.MAX_VALUE * 2L; i++) {
            int     value   = random.nextInt(1024 * sampleFactor);
            Integer integer = pool.getOrAddInteger(value);

            if (value != integer.intValue()) {
                System.err.println("wrong");
            }

            if (i %10000000 == 0 && i > 0) {
                System.out.println("done: " + i);

                for (int j = 0; j <pool.objectKeyTable.length; j++) {
                    Object o = pool.objectKeyTable[j];

                    if (o == null) {
                        continue;
                    }

                    int lookup = pool.getLookup(o, o.hashCode());

                    if (lookup == -1 || pool.objectKeyTable[lookup] != o) {
                        System.err.println("wrong");
                    }
                }
            }
        }

        System.out.println("hits: " + pool.hits);
        System.out.println("time: " +  (System.currentTimeMillis() - millis));
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(ValuePoolHashMapTest.class);

        return suite;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
