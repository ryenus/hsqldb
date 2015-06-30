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
package org.hsqldb.lib;

import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;
import junit.framework.Test;
import junit.framework.TestSuite;

@ForSubject(ArrayUtil.class)
public class ArrayUtilTest extends BaseTestCase {

    @OfMethod({"copyArray(java.lang.Object,java.lang.Object,int)",
        "sortArray(java.lang.Object)",
        "haveEqualSets(java.lang.Object,java.lang.Object,int)",
        "reorderMaps([I,[I)"})
    public void testArrayUtil() {

        int[] a = new int[] {
            23, 11, 37, 7, 1, 5
        };
        int[] b = new int[] {
            1, 3, 7, 11, 13, 17, 19, 3, 1
        };
        int[] c = ArrayUtil.toAdjustedColumnArray(a, 7, -1);
        int[] d = ArrayUtil.toAdjustedColumnArray(b, 11, 1);
        int[] e = new int[a.length];

        ArrayUtil.copyArray(a, e, a.length);
        ArrayUtil.sortArray(e);

        int[] f = new int[b.length];

        ArrayUtil.copyArray(b, f, b.length);
        ArrayUtil.sortArray(f);

        boolean x = ArrayUtil.haveEqualSets(a, e, a.length);
        boolean y = ArrayUtil.haveEqualSets(b, f, b.length);

        System.out.println("test passed: ");
        System.out.println(x == true && y == true && c.length == a.length - 1
                && d.length == b.length);

        // test copy
        int[] z = new int[b.length];

        b = new int[] {
            1, 3, 5, 7, 11, 13, 17, 19, 23
        };

        System.out.println(StringUtil.arrayToString(b));
        ArrayUtil.copyMoveSegment(b, z, b.length, 2, 3, 5);
        System.out.println(StringUtil.arrayToString(z));
        ArrayUtil.fillArray(z, 0);
        ArrayUtil.copyMoveSegment(b, z, b.length, 6, 3, 5);
        System.out.println(StringUtil.arrayToString(z));
        ArrayUtil.fillArray(z, 0);
        ArrayUtil.copyMoveSegment(b, z, b.length, 6, 2, 1);
        System.out.println(StringUtil.arrayToString(z));
        ArrayUtil.fillArray(z, 0);
        ArrayUtil.copyMoveSegment(b, z, b.length, 0, 3, 6);
        System.out.println(StringUtil.arrayToString(z));

        // test reorderMaps
        int[] arr0 = new int[] {
            5, 0, 9, 3
        };
        int[] arr1 = new int[] {
            0, 3, 5, 9
        };
        int[] arr2 = new int[] {
            12, 13, 14, 15
        };

        System.out.println(StringUtil.arrayToString(arr0));
        System.out.println(StringUtil.arrayToString(arr1));
        System.out.println(StringUtil.arrayToString(arr2));
        ArrayUtil.reorderMaps(arr0, arr1, arr2);
        System.out.println(StringUtil.arrayToString(arr1));
        System.out.println(StringUtil.arrayToString(arr2));
    }

    public static Test suite() {

        TestSuite suite = new TestSuite(ArrayUtilTest.class);

        return suite;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
