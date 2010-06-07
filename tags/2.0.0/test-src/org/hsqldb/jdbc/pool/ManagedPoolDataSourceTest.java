package org.hsqldb.jdbc.pool;

import junit.framework.TestCase;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Jakob Jenkov - Copyright 2005 Jenkov Development
 */
public class ManagedPoolDataSourceTest extends TestCase {
    
    static String url = "jdbc:hsqldb:mem:test"; //"jdbc:hsqldb:hsql://localhost/my_db";
    
    static ManagedPoolDataSource newManagedPoolDataSource(int size) throws Exception {
        return new ManagedPoolDataSource(url, "sa", "", size);
    }


    /**
     * Tests
     *
     * @throws Exception
     */
    public void testGetConnection_uniqueSessionConnectionWrappers() throws Exception {
        System.out.println("getConnection_uniqueSessionConnectionWrappers");
        
        Connection[] connections1 = new Connection[10];
        Connection[] connections2 = new Connection[connections1.length];
        ManagedPoolDataSource poolDataSource = newManagedPoolDataSource(connections1.length);

        System.out.println("--- 1 ---");
        
        for (int i = 0; i < connections1.length; i++) {
            connections1[i] = poolDataSource.getConnection();
            
            assertTrue(connections1[i] instanceof SessionConnectionWrapper);
        }
        
        assertEquals(10, poolDataSource.size());
        
        for (int i=0; i < connections1.length; i++){
            try {
                connections1[i].close();
            } catch (SQLException ex) {
                //ex.printStackTrace();
            }
            
            try {
                connections1[i].prepareStatement("blablab");
                fail("Should throw exception");
            } catch (SQLException e) {
                //ignore, expected after the connection is closed.
                System.out.println("Message: " + e.getMessage());
                System.out.println("SQLState: " + e.getSQLState());
                System.out.println("Error Code: " + e.getErrorCode());

                //assertEquals("This connection is closed.", e.getMessage());
            }
        }

        System.out.println("--- 2 ---");
        
        for (int i = 0; i < connections2.length; i++) {
            connections2[i] = poolDataSource.getConnection();
            
            assertNotSame(connections1[i], connections2[i]);
            
            try {
                connections2[i].close();
            } catch (SQLException ex) {
                //ex.printStackTrace();
            }
        }
    }

    public void testGetConnection_takeAvailableBeforeCreatingNew() throws Exception {
        System.out.println("getConnection_takeAvailableBeforeCreatingNew");
        Connection[] connections1 = new Connection[10];
        ManagedPoolDataSource poolDataSource = newManagedPoolDataSource(connections1.length);

        System.out.println("--- 1 ---");
        for (int i = 0; i < connections1.length; i++) {
            connections1[i] = poolDataSource.getConnection();
            assertTrue(connections1[i] instanceof SessionConnectionWrapper);
            
            try {
                connections1[i].close();
            } catch (SQLException ex) {
                //ex.printStackTrace();
            }
            
            assertEquals(1, poolDataSource.size());
        }
    }

    public void testDefaults() throws Exception {
        System.out.println("defaults");
        Connection[] connections1 = new Connection[1];
        DataSource poolDataSource = newManagedPoolDataSource(connections1.length);

        Connection connection1 = poolDataSource.getConnection();
        connection1.setAutoCommit(false);
//        int holdability = connection1.getHoldability();
        //connection1.setHoldability(holdability + 10); //not supported.
        int transactionIsolation = connection1.getTransactionIsolation();
        //connection1.setTransactionIsolation(transactionIsolation + 1);
        boolean isReadOnly = connection1.isReadOnly();
        connection1.setReadOnly(!isReadOnly);
        String catalog = connection1.getCatalog();
        connection1.setCatalog("somethingElse");
        
        try {
            connection1.close();
        } catch (SQLException ex) {
            //ex.printStackTrace();
        }

        Connection connection2 = poolDataSource.getConnection();
        assertTrue(connection2.getAutoCommit());
//        assertEquals(holdability, connection2.getHoldability()); //not supported
        assertEquals(transactionIsolation, connection2.getTransactionIsolation());
        assertEquals(isReadOnly, connection2.isReadOnly());
        assertEquals(catalog, connection2.getCatalog());


    }

    public void testFatalSQLException() throws Exception {
        //not decided how it should work yet.
    }

    public void testLoginTimeout() throws Exception {

    }

    public void testSessionTimeout() throws Exception {

    }


}
