import java.sql.*;
import java.util.Properties;

public class Test {
    public static void main(String[] args) {
        try {
            Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
        }
        catch (ClassNotFoundException e) {
            System.out.println("ERROR: can't find driver " + e.getMessage());
            return;
        }

        String dbUrl = "jdbc:arrow-flight://127.0.0.1:8900";
        String user= "root";
        String password = "";

        Properties info = new Properties();
        info.setProperty("user", user);
        info.setProperty("password", password);
        info.setProperty("useEncryption", "false");
        try {
            Connection connection = DriverManager.getConnection(dbUrl, info);
            // Connection connection = DriverManager.getConnection(dbUrl, username, password);
            String name = connection.getMetaData().getDatabaseProductName();
            System.out.println("getDatabaseProductName = " + name);

            // simple select
            Statement statement = connection.createStatement();
            String sql = "select version()";
            System.out.println(sql);
            ResultSet resultSet = statement.executeQuery(sql);
            while(resultSet.next()) {
                System.out.println(resultSet.getString("version()"));
            }
            resultSet.close();

            // create table
            sql = "drop table if exists test1";
            System.out.println(sql);
            int affected_rows = statement.executeUpdate(sql);
            System.out.println(affected_rows);

            sql = "create table test1(a string)";
            System.out.println(sql);
            affected_rows = statement.executeUpdate(sql);
            System.out.println(affected_rows);

            try {
                System.out.println(sql);
                statement.executeUpdate(sql);
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }

            sql = "insert into test1(a) values ('hello'), ('world')";
            System.out.println(sql);
            affected_rows = statement.executeUpdate(sql);
            System.out.println(affected_rows);

            sql = "select * from test1";
            resultSet = statement.executeQuery(sql);
            System.out.println(sql);
            while(resultSet.next()) {
                System.out.println(resultSet.getString(1));
                System.out.println(resultSet.getString("a"));
            }
            resultSet.close();
            statement.close();
            connection.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
            return;
        }
    }
}
