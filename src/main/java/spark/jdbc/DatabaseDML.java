package spark.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import scala.Tuple2;
import spark.jdbc.DatabaseConn;

public class DatabaseDML {

    public static void insertDB(String name, String result) {
        String query = "insert into sparkFunctions (sp_name,sp_result) values (?,?);";
        System.out.println(query);
        try(Connection conn = DatabaseConn.getConnection();
        PreparedStatement preStatement = conn.prepareStatement(query);)
        {
            preStatement.setString(1,name);
            preStatement.setString(2,result);
            preStatement.addBatch();
            preStatement.executeBatch();
        } catch(SQLException ex) {
            System.err.println(ex.getMessage());
        }
    }

	public static void insertDB(String name, long count) {
        String query = "insert into sparkFunctions (sp_name,sp_result) values (?,?);";
        System.out.println(query);
        try(Connection conn = DatabaseConn.getConnection();
        PreparedStatement preStatement = conn.prepareStatement(query);)
        {
            preStatement.setString(1,name);
            preStatement.setLong(2,count);
            preStatement.addBatch();
            preStatement.executeBatch();
        } catch(SQLException ex) {
            System.err.println(ex.getMessage());
        }
	}

	public static void insertDB(String name, double result) {
        String query = "insert into sparkFunctions (sp_name,sp_result) values (?,?);";
        System.out.println(query);
        try(Connection conn = DatabaseConn.getConnection();
        PreparedStatement preStatement = conn.prepareStatement(query);)
        {
            preStatement.setString(1,name);
            preStatement.setDouble(2, result);
            preStatement.addBatch();
            preStatement.executeBatch();
        } catch(SQLException ex) {
            System.err.println(ex.getMessage());
        }
	}

}