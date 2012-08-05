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

/*
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @author Sergio Bossa (sbtourist@users dot sourceforge.net)
 * @version 2.2.9
 * @since 2.2.9
*/
package org.hsqldb.lib;

import java.util.Random;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class DoubleIntIndexTest extends TestCase {

    public void testIndexSortingWithOneNumber() {

        DoubleIntIndex index = new DoubleIntIndex(1, true);

        index.addUnsorted(1, 1);
        assertEquals(1, index.lookup(1));
    }

    public void testIndexSortingWithFixedNumbers() {

        DoubleIntIndex index = new DoubleIntIndex(2, true);

        index.addUnsorted(2, 2);
        index.addUnsorted(1, 1);
        assertEquals(1, index.lookup(1));
    }

    public void testIndexSortingWithRandomNumbers() {

        Random r = new Random();

        for (int i = 0; i < 1000; i++) {
            testIndexSortedInsertWithRandomSizesAndNumbers(r.nextInt(100000));
        }

        testIndexSortedInsertWithRandomSizesAndNumbers(0);
        testIndexSortedInsertWithRandomSizesAndNumbers(1);
        testIndexSortedInsertWithRandomSizesAndNumbers(2);
    }

    private void testIndexSortedInsertWithRandomSizesAndNumbers(int total) {

        DoubleIntIndex index = new DoubleIntIndex(total, true);
        Random         r     = new Random();

        for (int i = 0; i < total; i++) {
            int kv = r.nextInt();

            index.addUnsorted(kv, kv);
        }

        index.sort();

        int lastValue = Integer.MIN_VALUE;

        for (int i = 0; i < total; i++) {
            int key = index.getKey(i);

            super.assertTrue("wrong sort", key >= lastValue);

            lastValue = key;
        }
    }

    public static Test suite() {

        TestSuite suite = new TestSuite(DoubleIntIndexTest.class);

        return suite;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
