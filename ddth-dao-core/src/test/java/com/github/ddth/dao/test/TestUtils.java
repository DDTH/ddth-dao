package com.github.ddth.dao.test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

public class TestUtils {
    private static Properties sysProps = new Properties();
    static {
        System.getProperties().forEach((k, v) -> sysProps.put(k.toString().toUpperCase(), v));
    }

    /**
     * Check if a system property exists (case-insensitive).
     * 
     * @param key
     * @return
     */
    public static boolean hasSystemPropertyIgnoreCase(String key) {
        return sysProps.containsKey(key.toUpperCase());
    }

    /**
     * Get a system property (case-insensitive).
     * 
     * @param key
     * @param defaultValue
     * @return
     */
    public static String getSystemPropertyIgnoreCase(String key, String defaultValue) {
        return sysProps.getProperty(key, defaultValue);
    }

    /**
     * Load and run a SQL script.
     * 
     * @param conn
     * @param scriptPath
     * @param replacements
     * @throws IOException
     * @throws SQLException
     */
    public static void runSqlScipt(Connection conn, String scriptPath,
            Map<String, String> replacements) throws IOException, SQLException {
        try (InputStream is = TestUtils.class.getResourceAsStream(scriptPath)) {
            List<String> lines = IOUtils.readLines(is, "UTF-8");
            String SQL = "";
            for (String line : lines) {
                if (line.startsWith("#") || line.startsWith("--")) {
                    continue;
                }
                SQL += line;
                if (line.endsWith(";")) {
                    for (Entry<String, String> entry : replacements.entrySet()) {
                        SQL = StringUtils.replace(SQL, entry.getKey(), entry.getValue());
                    }
                    conn.createStatement().execute(SQL);
                    SQL = "";
                }
            }
        }
    }

    public static DataSource buildDataSource() throws SQLException {
        DataSource ds = buildMySqlDataSource();
        ds = ds != null ? ds : buildPgSqlDataSource();
        return ds;
    }

    public static DataSource buildMySqlDataSource() throws SQLException {
        String hostAndPort = System.getProperty("mysql.hostAndPort");
        if (hostAndPort == null) {
            return null;
        }
        String user = System.getProperty("mysql.user", "root");
        String password = System.getProperty("mysql.pwd", "");
        String db = System.getProperty("mysql.db", "test");
        String url = "jdbc:mysql://" + hostAndPort + "/" + db
                + "?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&useSSL=false&useLegacyDatetimeCode=false&serverTimezone=Asia/Ho_Chi_Minh";
        return new SimpleDriverDataSource(DriverManager.getDriver(url), url, user, password);
    }

    public static DataSource buildPgSqlDataSource() throws SQLException {
        String hostAndPort = System.getProperty("pgsql.hostAndPort");
        if (hostAndPort == null) {
            return null;
        }
        String user = System.getProperty("pgsql.user", "postgres");
        String password = System.getProperty("pgsql.pwd", "");
        String db = System.getProperty("pgsql.db", "test");
        String url = "jdbc:postgresql://" + hostAndPort + "/" + db + "?ssl=false";
        return new SimpleDriverDataSource(DriverManager.getDriver(url), url, user, password);
    }
}
