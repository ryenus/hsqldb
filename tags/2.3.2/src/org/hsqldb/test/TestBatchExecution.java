/* Copyright (c) 2001-2014, The HSQL Development Group
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


package org.hsqldb.test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.hsqldb.lib.StopWatch;

/**
 * A quick test of the new CompiledStatement and batch execution facilities.
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */

// fredt@users - modified to do some network connection and generated result tests
public class TestBatchExecution extends TestBase {

    static final String drop_table_sql = "drop table test if exists";
    static final String create_cached  = "create cached ";
    static final String create_memory  = "create memory ";
    static final String create_temp    = "create temp ";
    static final String table_sql = "table test(id int identity primary key,"
                                    + "fname varchar(20), lname "
                                    + "varchar(20), zip int)";
    static final String insert_sql = "insert into test values(?,?,?,?)";
    static final String update_sql =
        "update test set fname = 'Hans' where id = ?";
    static final String select_sql   = "select * from test where id = ?";
    static final String delete_sql   = "delete from test where id = ?";
    static final String call_sql     = "call identity()";
    static final String shutdown_sql = "shutdown compact";
    static final String def_db_path  = "batchtest";
    static final int    def_runs     = 5;
    static final int    rows         = 10000;
    static Connection   conn;
    static Statement    stmnt;
    static String       url;

    public TestBatchExecution(String name) {
        super(name);
    }

    public void test() throws Exception {

        conn  = newConnection();
        stmnt = conn.createStatement();
        url   = super.url;

        nonPreparedTest();
        preparedTestOne(5);
    }

    static void print(String s) {
        System.out.print(s);
    }

    static void println(String s) {
        System.out.println(s);
    }

    static void printCommandStats(StopWatch sw, String cmd, int count) {

        long et = sw.elapsedTime();

        print(sw.elapsedTimeToMessage(count + " " + cmd));
        println(" " + ((1000 * count) / et) + " ops/s.");
    }

    public static void main(String[] args) throws Exception {

        int    runs;
        String db_path;
        Driver driver;

        runs    = def_runs;
        db_path = def_db_path;

        try {
            runs = Integer.parseInt(args[0]);
        } catch (Exception e) {}

        db_path = "batchtest";

        try {
            db_path = args[1];
        } catch (Exception e) {}

        // get the connection and statement
        driver =
            (Driver) Class.forName("org.hsqldb.jdbc.JDBCDriver").newInstance();

        DriverManager.registerDriver(driver);

        url = "jdbc:hsqldb:file:" + db_path
              + ";crypt_key=604a6105889da65326bf35790a923932;crypt_type=blowfish;hsqldb.default_table_type=cached;hsqldb.cache_rows=100"
        ;
        conn  = DriverManager.getConnection(url, "SA", "");
        stmnt = conn.createStatement();

        runTests(runs);
    }

    static void runTests(int runs) throws Exception {

        println("");
        println("***************************************");
        println("featuring cached (persistent) table");
        println("***************************************");

        // drop and recreate the test table
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println(create_cached + table_sql);
        stmnt.execute(create_cached + table_sql);
        preparedTestOne(runs);

        // drop the test table and shut down database
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println("---------------------------------------");
        println("shutting down database");
        stmnt.execute(shutdown_sql);
        println("---------------------------------------");

        // get the connection and statement
        conn  = DriverManager.getConnection(url, "SA", "");
        stmnt = conn.createStatement();

        println("");
        println("***************************************");
        println("featuring memory (persistent) table");
        println("***************************************");

        // drop and recreate the test table
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println(create_memory + table_sql);
        stmnt.execute(create_memory + table_sql);
        preparedTestOne(runs);

        // drop the test table and shut down database
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println("---------------------------------------");
        println("shutting down database");
        stmnt.execute(shutdown_sql);
        println("---------------------------------------");

        // get the connection and statement
        conn  = DriverManager.getConnection(url, "SA", "");
        stmnt = conn.createStatement();

        println("");
        println("***************************************");
        println("featuring temp (transient) table");
        println("***************************************");

        // drop and recreate the test table
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println(create_temp + table_sql);
        stmnt.execute(create_temp + table_sql);
        preparedTestOne(runs);

        // drop the test table
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println("---------------------------------------");
        println("shutting down database");
        stmnt.execute(shutdown_sql);
        println("---------------------------------------");

        //
        preparedTestTwo();
        preparedTestThree();
    }

    public static void nonPreparedTest() throws Exception {

        stmnt.addBatch(drop_table_sql);
        stmnt.addBatch(create_memory + table_sql);
        stmnt.executeBatch();
    }

    public static void preparedTestOne(int runs) throws Exception {

        PreparedStatement insertStmnt;
        PreparedStatement updateStmnt;
        PreparedStatement selectStmnt;
        PreparedStatement deleteStmnt;
        PreparedStatement callStmnt;
        StopWatch         sw;

        println("---------------------------------------");
        println("Preparing Statements:");
        println("---------------------------------------");
        println(insert_sql);
        println(update_sql);
        println(select_sql);
        println(delete_sql);
        println(call_sql);

        sw = new StopWatch();

        // prepare the statements
        insertStmnt = conn.prepareStatement(insert_sql,
                                            Statement.RETURN_GENERATED_KEYS);
        updateStmnt = conn.prepareStatement(update_sql);
        selectStmnt = conn.prepareStatement(select_sql);
        deleteStmnt = conn.prepareStatement(delete_sql);
        callStmnt   = conn.prepareCall(call_sql);

        println("---------------------------------------");
        println(sw.elapsedTimeToMessage("statements prepared"));
        println("---------------------------------------");
        sw.zero();

        // set up the batch data
        for (int i = 0; i < rows; i++) {
            insertStmnt.setInt(1, i);
            insertStmnt.setString(2, "Julia");
            insertStmnt.setString(3, "Peterson-Clancy");
            insertStmnt.setInt(4, i);
            updateStmnt.setInt(1, i);
            selectStmnt.setInt(1, i);
            deleteStmnt.setInt(1, i);
            insertStmnt.addBatch();
            updateStmnt.addBatch();
            selectStmnt.addBatch();
            deleteStmnt.addBatch();
            callStmnt.addBatch();
        }

        println("---------------------------------------");
        println(sw.elapsedTimeToMessage("" + 5 * rows
                                        + " batch entries created"));
        sw.zero();

        // do the test loop forever
        for (int i = 0; i < 1; i++) {
            println("---------------------------------------");

            // inserts
            sw.zero();
            insertStmnt.executeBatch();
            printCommandStats(sw, "inserts", rows);

            ResultSet    generated = insertStmnt.getGeneratedKeys();
            StringBuffer sb        = new StringBuffer();

            int genCount = 0;
            while (generated.next()) {
                int gen = generated.getInt(1);

                if (gen % 1000 == 0) {
                    sb.append(gen).append(" - ");
                }

                genCount++;
            }

            System.out.println(sb.toString());
            printCommandStats(sw, "generated reads", genCount);

            // updates
            sw.zero();
            int[] updateCounts = updateStmnt.executeBatch();
            printCommandStats(sw, "updates", updateCounts.length);

            // selects
            sw.zero();

//            selectStmnt.executeBatch();
//            printCommandStats(sw, "selects");
            // deletes
            sw.zero();
            updateCounts = deleteStmnt.executeBatch();
            printCommandStats(sw, "deletes", updateCounts.length);

            // calls
            sw.zero();

//            callStmnt.executeBatch();
//            printCommandStats(sw, "calls  ");
        }
    }

    public static void preparedTestTwo() {

        System.out.println("preparedTestTwo");

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            Connection con = DriverManager.getConnection("jdbc:hsqldb:mem:.",
                "sa", "");

            System.out.println("con=" + con);

            Statement stmt = con.createStatement();

            try {
                stmt.executeUpdate("drop table ttt");
            } catch (Exception e) {}

            stmt.executeUpdate("create table ttt (id integer)");

            PreparedStatement prep =
                con.prepareStatement("INSERT INTO ttt (id) VALUES (?)");

            con.setAutoCommit(false);

            for (int i = 1; i <= 4; i++) {    // [2, 3, 4]
                prep.setInt(1, i);
                prep.addBatch();
                System.out.println("executeBatch() for " + i);
                prep.executeBatch();
                con.commit();

                // prep.clearBatch(); // -> java.lang.NullPointerException
                // at org.hsqldb.Result.getUpdateCounts(Unknown Source)
            }

            prep.close();

            // see what we got
            ResultSet rs = stmt.executeQuery("select * from ttt");

            while (rs.next()) {
                System.out.println("id = " + rs.getInt(1));
            }

            System.out.println("bye.");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void preparedTestThree() {

        System.out.println("preparedTestThree");

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            Connection con = DriverManager.getConnection("jdbc:hsqldb:mem:.",
                "sa", "");

            con.setAutoCommit(false);
            System.out.println("con=" + con);

            Statement stmt = con.createStatement();

            try {
                stmt.executeUpdate("drop table node");
            } catch (Exception e) {}

            stmt.executeUpdate(
                "create table Node (id varbinary(255) not null, name varchar(255), primary key (id))");

            PreparedStatement prep = con.prepareStatement(
                "insert into Node (name, id) values (?, ?)");
            byte[] byteArray = null;

            try {
                byteArray =
                    org.hsqldb.lib.StringConverter.hexStringToByteArray(
                        "c0a8000a30d110808130d18080880000");
            } catch (Exception e) {
                //
            }


            prep.setNull(1, java.sql.Types.VARCHAR);
            prep.setBytes(2, byteArray);

            int result = prep.executeUpdate();

            prep.close();

            prep = con.prepareStatement("delete from Node where id=?");

            prep.setBytes(1, byteArray);
            prep.addBatch();
            System.out.println("executeBatch() for delete");
            prep.executeBatch();
            con.commit();

            // prep.clearBatch(); // -> java.lang.NullPointerException
            // at org.hsqldb.Result.getUpdateCounts(Unknown Source)
            prep.close();

            // see what we got
            ResultSet rs = stmt.executeQuery("select * from Node");

            while (rs.next()) {
                System.out.println("row retreived");
            }

            System.out.println("bye.");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
