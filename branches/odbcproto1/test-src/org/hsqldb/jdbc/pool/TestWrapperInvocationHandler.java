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


package org.hsqldb.jdbc.pool;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLData;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashSet;
import java.util.Set;

/**
* Displays methods of classes in the JDBC4 java.sql package that may be
* sensitive to Connection or Statement pooling.
* <P/>
* Was used to help 'visually' verify the handler above covers all the bases
* package org.hsqldb.jdbc.pool;
*
* @author boucherb@users
*/
public class TestWrapperInvocationHandler {
//public class DisplayMethodsPotentiallySensitiveToPooling {  original Name

    static Class[] getSensitiveReturnTypeClasses() {
        return new Class[]  {
            Array.class,
            CallableStatement.class,
            Connection.class,
            DatabaseMetaData.class,
            PreparedStatement.class,
            ResultSet.class,
            Statement.class
        };
    }

    static Set sensitiveReturnTypeSet;

    static Set getSensitiveReturnTypeSet() {
        if (sensitiveReturnTypeSet == null) {

            Set set = new HashSet();

            Class[] classes = getSensitiveReturnTypeClasses();

            for (int i = 0; i < classes.length; i++) {
                set.add(classes[i]);
            }

            sensitiveReturnTypeSet = set;
        }

        return sensitiveReturnTypeSet;
    }

    static Class[] getSqlClasses() {
        return new Class[]  {
            Array.class,
            Blob.class,
            CallableStatement.class,
            Clob.class,
            Connection.class,
            DatabaseMetaData.class,
            ParameterMetaData.class,
            PreparedStatement.class,
            Ref.class,
            ResultSet.class,
            ResultSetMetaData.class,
            RowId.class,
            SQLXML.class,
            Statement.class,
            Struct.class,
            SQLData.class,
            SQLInput.class,
            SQLOutput.class,
            Savepoint.class
        };
    }

    public static void main(String[] args) {
        doWork();
    }

    static void doWork() {
        Class[] classes = getSqlClasses();

        for (int i = 0; i < classes.length; i++) {
            processClass(classes[i]);
        }
    }


    static void processClass(Class clazz) {

        Method[] methods = clazz.getMethods();

        boolean found = false;

        for (int i = 0; i < methods.length; i++) {
            if (processMethod(found, clazz, methods[i])) {
                found = true;
            }
        }

        if (found) {
            System.out.println("*****    " + clazz + "    *****");
            System.out.println();
        }
    }

    static boolean processMethod(boolean found, Class clazz, Method method) {

        if (!Modifier.isPublic(method.getModifiers())) {
            return false;
        }

        Class returnType = method.getReturnType();
        Set   set        = getSensitiveReturnTypeSet();

        if (set.contains(returnType)
                || "close".equals(method.getName())
                || "isClosed".equals(method.getName())
                || "isWrapperFor".equals(method.getName())
                || "unwrap".equals(method.getName())) {

            if (!found) {
                found = true;

                System.out.println("*****    " + clazz + "    *****");
            }

            System.out.println(method);

        }

        return found;
    }
}
