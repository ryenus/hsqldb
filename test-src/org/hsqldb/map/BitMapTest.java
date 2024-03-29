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
package org.hsqldb.map;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.hsqldb.testbase.BaseTestCase;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

@ForSubject(BitMap.class)
public class BitMapTest extends BaseTestCase {

    @OfMethod({"leftShift(byte[],int)","and(byte[],byte,int)"})
    public void testByteBitMap() {

        int value;

        byte[] map = new byte[]{
            0, 0, (byte) 255, (byte) 255, 0, 0
        };

        map = BitMap.leftShift(map, 1);
        map = BitMap.leftShift(map, 4);
        map = BitMap.leftShift(map, 8);

        BitMap.and(map, 24, (byte) 0x80, 1);
        BitMap.and(map, 8, (byte) 0x80, 1);
        BitMap.and(map, 12, (byte) 0xff, 8);
        BitMap.or(map, 24, (byte) 0x80, 1);
        BitMap.or(map, 8, (byte) 0x80, 1);
        BitMap.or(map, 12, (byte) 0xff, 8);
        BitMap.and(map, 24, (byte) 0, 1);
        BitMap.and(map, 8, (byte) 0, 1);
        BitMap.and(map, 12, (byte) 0, 8);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(BitMapTest.class);

        return suite;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
