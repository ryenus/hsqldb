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


package org.hsqldb.jdbc;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.hsqldb.testbase.ForSubject;
import org.hsqldb.testbase.OfMethod;

/**
 * Test of HSQLDB specific extensions to the interface java.sql.Statement.
 */
@ForSubject(Statement.class)
public class HsqldbExtensionsToJDBCStatementTest extends JDBCStatementTest {

    public HsqldbExtensionsToJDBCStatementTest(String testName) {
        super(testName);
    }

    /**
     * Test of getGeneratedKeys method, of interface java.sql.Statement using
     * the org.hsqldb.result.ResultConstants.RETURN_PRIMARY_KEYS flag on
     * java.sql.Statement.execute(String, int).
     */
    @OfMethod("getGeneratedKeys()")
    public void testGetGeneratedKeysWith_RETURN_PRIMARY_KEYS_flag() throws Exception {
        Statement stmt = newStatement();
        
        int       expectedCount;
        boolean   executeResult;
        ResultSet generatedKeys;
        
        //
        // automatic return of composed primary key
        // 
        // Comparator to required for the ordering of the primary keys
        class PKComparator implements Comparator<Map.Entry<Integer,Integer>> {
                    @Override
                    public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                        final int comparedInvoiceId = 
                                Integer.compare(o1.getKey(), o2.getKey());
                        if (comparedInvoiceId == 0) {
                            // first is equal, thus we need to compare the 2nd
                            return Integer.compare(o1.getValue(), o2.getValue());
                        }
                        return comparedInvoiceId;
                    }
                }
        // a sorted set containing the expected primary key values
        final Set<Map.Entry<Integer,Integer>> expectedTmp = 
                new TreeSet<Map.Entry<Integer, Integer>>(new PKComparator());
        // populate the expected set
        final ResultSet primaryKeyValuesForItemsWithProductIdZero = stmt.executeQuery(
                "select   INVOICEID, ITEM " + 
                "from     item " + 
                "where    PRODUCTID=0"
        );
        while (primaryKeyValuesForItemsWithProductIdZero.next()) {
            final int invoiceId = primaryKeyValuesForItemsWithProductIdZero.getInt(1);
            final int item      = primaryKeyValuesForItemsWithProductIdZero.getInt(2);
            expectedTmp.add(new AbstractMap.SimpleImmutableEntry<Integer, Integer>(invoiceId, item));
        }
        primaryKeyValuesForItemsWithProductIdZero.close();
        final Set<Map.Entry<Integer,Integer>> expected = 
                Collections.unmodifiableSet(expectedTmp);
        expectedCount = expected.size();
        
        // now to the test:
        executeResult = stmt.execute(
                "update item " +
                "set COST=99999" +
                "where PRODUCTID=0", 
                org.hsqldb.result.ResultConstants.RETURN_PRIMARY_KEYS);
        
        assertFalse("UPDATE statement execution returned a result set.", 
                executeResult);
        
        assertEquals("Number of affected rows differs.", 
                expectedCount, stmt.getUpdateCount());
        
        
        // a sorted set containing the actual primary key values
        final Set<Map.Entry<Integer,Integer>> actual = 
                new TreeSet<Map.Entry<Integer, Integer>>(new PKComparator());
        generatedKeys = stmt.getGeneratedKeys();
        while (generatedKeys.next()) {
            final int invoiceId = generatedKeys.getInt(1);
            final int item      = generatedKeys.getInt(2);
            actual.add(new AbstractMap.SimpleImmutableEntry<Integer, Integer>(invoiceId, item));
        }
        generatedKeys.close();
        
        // compare actual with expected
        assertEquals("The sets differ in size.", expected.size(), actual.size());
        
        Iterator<Map.Entry<Integer,Integer>> expectedIt = expected.iterator();
        Iterator<Map.Entry<Integer,Integer>> actualIt   = actual.iterator();
        while (expectedIt.hasNext() && actualIt.hasNext()) {
            Map.Entry<Integer,Integer> e = expectedIt.next();
            Map.Entry<Integer,Integer> a = actualIt.next();
            assertEquals("INVOICEID (key) is not the same", 
                    e.getKey(),   a.getKey());
            assertEquals("ITEM (value) is not the same", 
                    e.getValue(), a.getValue());
        }
        assertEquals(expectedIt.hasNext(), actualIt.hasNext());
    }


    public static void main(java.lang.String[] argList) {

        junit.textui.TestRunner.run(suite());
    }

}
