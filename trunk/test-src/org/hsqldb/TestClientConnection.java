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


package org.hsqldb;

public class TestClientConnection {
    /**
     * Quick & Dirty Unit test of Network Compatibility utility methods.
     * Move to dedicated Testing class in test package
     */
    public static void main(String[] sa) {

        if (!ClientConnection.toNcvString(-2030405).equals("2.3.4.5")) {
            throw new RuntimeException("Test of int -2030405 failed");
        }

        if (!ClientConnection.toNcvString(-23456789).equals("23.45.67.89")) {
            throw new RuntimeException("Test of int -23456789 failed");
        }

        if (!ClientConnection.toNcvString(-2).equals("0.0.0.2")) {
            throw new RuntimeException("Test of int -2 failed");
        }

        if (!ClientConnection.toNcvString(-300000).equals("0.30.0.0")) {
            throw new RuntimeException("Test of int -300000 failed");
        }

        if (ClientConnection.toNcvInt("9.80.76.5") != -9807605) {
            throw new RuntimeException("Test of String '9.80.76.5' failed");
        }

        if (ClientConnection.toNcvInt("23.45.67.89") != -23456789) {
            throw new RuntimeException("Test of String '23.45.67.89' failed");
        }

        if (ClientConnection.toNcvInt("0.0.0.2") != -2) {
            throw new RuntimeException("Test of String '0.0.0.2' failed");
        }
    }
}
