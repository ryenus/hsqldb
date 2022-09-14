/* Copyright (c) 2001-2021, The HSQL Development Group
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

import java.util.Comparator;
import java.util.Objects;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 *
 * @author Campbell Burnet (campbell-burnet@users dot sourceforge.net)
 */
@ForSubject(HsqlArrayHeap.class)
@SuppressWarnings("ClassWithoutLogger")
public class HsqlArrayHeapTest extends BaseTestCase {

    public static Test suite() {
        return new TestSuite(HsqlArrayHeapTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public HsqlArrayHeapTest(String name) {
        super(name);
    }

    @OfMethod({"add(java.lang.Integer)", "size()", "remove()", "peek()", "isEmpty()"})
    @SuppressWarnings("UnnecessaryBoxing")
    public void testHsqlArrayHeap() {

        @SuppressWarnings("Convert2Lambda")
        Comparator<Integer> oc = new Comparator<Integer>() {

            @SuppressWarnings("UnnecessaryUnboxing")
            @Override
            public int compare(Integer a, Integer b) {

                if (Objects.equals(a, b)) {
                    return 0;
                }

                // null==null and smaller than any value
                if (a == null) {
                    if (b == null) {
                        return 0;
                    }

                    return -1;
                }

                if (b == null) {
                    return 1;
                }

                return a.intValue() - b.intValue();
            }
        };
        
        HsqlHeap<Integer> ah = new HsqlArrayHeap<>(6, oc);

        assertTrue("isEmpty()", ah.isEmpty());

        int[] ai = new int[]{
            3, 99, 7, 9, -42, 2, 1, 23, -7
        };
        int least = Integer.MIN_VALUE;

        for (int i = 0; i < ai.length; i++) {
            println("add()     : Integer.valueOf(" + ai[i] + ")");
            ah.add(Integer.valueOf(ai[i]));
            println("size()    : " + ah.size());
        }

        while (ah.size() > 0) {
            @SuppressWarnings("UnnecessaryUnboxing")
            int current = ah.remove().intValue();

            println("remove()  : " + current);
            println("size()    : " + ah.size());

            assertFalse("bad heap invariant: current < least", current < least);

            least = current;
        }

        assertNull("peak()", ah.peek());
        assertTrue("isEmpty()", ah.isEmpty());
        assertNull("remove()", ah.remove());
        assertEquals("size()", 0, ah.size());
        assertTrue("isEmpty()", ah.isEmpty());
        assertFalse("isFull()", ah.isFull());
    }
}
