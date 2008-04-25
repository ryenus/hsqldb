import java.sql.Connection;
import java.sql.SQLException;
import org.hsqldb.util.RCData;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Sample class which embeds SqlFile.
 * Suitable for using as a template.
 *
 * @see #main
 * @author blaine.simpson@admc.com
 */
public class SqlFileEmbedder {
    private Connection conn;

    public Connection getConn() {
        return conn;
    }


    /**
     * Run<PRE>
     *     java SqlFileEmbedder</PRE>
     * to see Syntax message.
     */
    public static void main(String[] sa) throws Exception {
        if (sa.length < 3) {
            System.err.println("SYNTAX:  " + SqlFileEmbedder.class.getName()
                    + " path/ro/file.rc URLID file1.sql...");
            System.exit(2);
        }
        SqlFileEmbedder embedder =
                new SqlFileEmbedder(new File(sa[0]), sa[1]);
        String[] files = new String[sa.length - 2];
        for (int i = 0; i < sa.length - 2; i++) {
            files[i] = sa[i + 2];
        }
        try {
            embedder.executeFiles(files);
        } finally {
            try {
                embedder.getConn().close();
            } catch (SQLException se) {}
        }
    }

    /**
     * Instantiates SqlFileEmbedder object and connects to specified database.
     *
     * N.b., you do not need to use RCData to use SqlFile.
     * All SqlFile needs is a live Connection.
     * I'm using RCData because it is a convenient way for a non-contained
     * app (i.e. one that doesn't run in a 3rd party container) to get a
     * Connection.
     */
    public SqlFileEmbedder(File rcFile, String urlid) throws Exception {
        conn = (new RCData(rcFile, urlid)).getConnection();
        conn.setAutoCommit(false);
    }

    public void executeFiles(String[] fileStrings)
            throws IOException, SqlToolError, SQLException {
        Map<String, String> sqlVarMap = new HashMap<String, String>();
        sqlVarMap.put("invoker", getClass().getName());
        // This variable is pretty useless, but this should show you how to
        // set variables which you can access inside of scripts like *{this}.

        File file;
        for (String fileString : fileStrings) {
            file = new File(fileString);
            if (!file.isFile())
                throw new IOException("SQL file not present: "
                        + file.getAbsolutePath());
            new SqlFile(file, false, sqlVarMap, null).execute(conn, null);
        }
    }
}
