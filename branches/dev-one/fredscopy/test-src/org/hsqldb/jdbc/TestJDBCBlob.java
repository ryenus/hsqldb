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


package org.hsqldb.jdbc;

public class TestJDBCBlob {
    public static void main(String[] args) throws Exception {

        System.out.println("--------------------------------");
        System.out.println((new JDBCBlob(new byte[0])).position(new byte[]{1}, 1));
        System.out.println((new JDBCBlob(new byte[]{1})).position(new byte[0], 1));
        System.out.println((new JDBCBlob(new byte[]{1})).position((byte[])null, 1));

        System.out.println("--------------------------------");
        byte[] data1 = new byte[]{0,1,2,1,2,3,2,3,4,2,3,4,5,2,3,4,5,0,1,2,
                                  1,2,3,2,3,4,2,3,4,5,2,3,4};
        byte[] pattern = new byte[]{2,3,4,5};

        JDBCBlob blob1 = new JDBCBlob(data1);
        JDBCBlob blob2 = new JDBCBlob(pattern);

        for (int i = -1; i <= data1.length + 1; i++) {
            System.out.println(blob1.position(pattern, i));
        }

        System.out.println("--------------------------------");

        for (int i = -1; i <= data1.length + 1; i++) {
            System.out.println(blob1.position(blob2, i));
        }

        System.out.println("--------------------------------");

        new JDBCBlob(null);
    }
}
