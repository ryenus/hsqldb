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
package org.hsqldb;

import org.hsqldb.testbase.ForSubject;

@ForSubject(ClientConnection.class)
public class ClientConnectionTest extends junit.framework.TestCase {

    /* TODO:  Test some ipv6 addresses.
     *        Only ipv4 addresses are tested at this time. */
    public void testSingleDigitIpv4Int() {
        assertEquals("2.3.4.5",
                     ClientConnection.toNetCompVersionString(-2030405));
    }

    public void testDoubleDigitIpv4Int() {
        assertEquals("23.45.67.89",
                     ClientConnection.toNetCompVersionString(-23456789));
    }

    public void test000DigitIpv4Int() {
        assertEquals("0.0.0.2", ClientConnection.toNetCompVersionString(-2));
    }

    public void testDoubleZeroesDigitIpv4Int() {
        assertEquals("0.30.0.0",
                     ClientConnection.toNetCompVersionString(-300000));
    }

//    public void testMixedDigitIpv4String() {
//        assertEquals(-9807605, ClientConnection.toNcvInt("9.80.76.5"));
//    }
//
//    public void testDoubleDigitIpv4String() {
//        assertEquals(-23456789, ClientConnection.toNcvInt("23.45.67.89"));
//    }
//
//    public void test000DigitIpv4String() {
//        assertEquals(-2, ClientConnection.toNcvInt("0.0.0.2"));
//    }

    /**
     * This method allows to easily run this unit test independent of the other
     * unit tests, and without dealing with Ant or unrelated test suites.
     */
    public static void main(String[] sa) {

        junit.textui.TestRunner runner = new junit.textui.TestRunner();
        junit.framework.TestResult result = junit.textui.TestRunner.run(
            runner.getTest(ClientConnectionTest.class.getName()));

        System.exit(result.wasSuccessful() ? 0
                                           : 1);
    }
}
