/* Copyright (c) 2001-2009, The HSQL Development Group
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


package org.hsqldb.lib.tar;

import java.io.IOException;
import java.io.File;

public class PIFGeneratorTest {

    /**
     * This is a Unit Test.  Move it to a proper, dedicated unit test class.
     */
    static public void main(String[] sa)
    throws TarMalformatException, IOException {

        if (sa.length > 1) {
            throw new IllegalArgumentException("java "
                                               + PIFGenerator.class.getName()
                                               + " [xTargetPath]");
        }

        PIFGenerator pif = (sa.length < 1) ? (new PIFGenerator(1))
                                           : (new PIFGenerator(
                                               new File(sa[0])));

        pif.addRecord("o", "n");                                           // Shortest possible
        pif.addRecord("k1", "23");                                         // total 8.  Impossible to get total of 9.
        pif.addRecord("k2", "234");                                        // total 10
        pif.addRecord("k3", "2345");                                       // total 11
        pif.addRecord("k4",
                      "2345678901234567890123456789012345678901234567890"
                      + "123456789012345678901234567890123456789012");     //total 98

        // Impossible to get total of 99.
        pif.addRecord("k5",
                      "2345678901234567890123456789012345678901234567890"
                      + "1234567890123456789012345678901234567890123");    //total 100
        pif.addRecord("int1234", 1234);
        pif.addRecord("long1234", 1234);
        pif.addRecord("boolTrue", true);
        pif.addRecord("boolFalse", false);
        System.out.println("Name (" + pif.getName() + ')');
        System.out.write(pif.toByteArray());
    }
}
