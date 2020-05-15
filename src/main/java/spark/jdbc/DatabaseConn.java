package spark.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseConn {
    private static String url;
    private static String username;
    private static String password;

    static {
//        url = System.getProperty("database.url", "jdbc:postgresql://18.191.164.64:5432/sparkdb");
        url = System.getProperty("database.url", "jdbc:postgresql://localhost:5432/sparkdb");
        username = System.getProperty("database.username", "sparkdb");
        password = System.getProperty("database.password", "sparkdb");
    }

    public static Connection getConnection() throws SQLException {
        //return DriverManager.getConnection("jdbc:postgresql://localhost:5432/mydb", "mydb", "mydb");
        return DriverManager.getConnection(url, username, password);
    }    

}