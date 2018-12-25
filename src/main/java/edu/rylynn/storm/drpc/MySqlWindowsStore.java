package edu.rylynn.storm.drpc;

import com.mysql.jdbc.Connection;
import org.apache.storm.trident.windowing.WindowsStore;

import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MySqlWindowsStore implements WindowsStore {
    private Connection connection = getConnection();

    private static Connection getConnection() {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://10.113.9.116:3306/order_db?useUnicode=true&characterEncoding=UTF-8";
        String username = "root";
        String password = "root";
        Connection conn = null;
        try {
            Class.forName(driver); //classLoader,加载对应驱动
            conn = (Connection) DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public Object get(String key) {
        String sqlStatement = "select skuSum from sku_window where skuName='%s';";
        String sql = new String(String.format(sqlStatement, key).getBytes(StandardCharsets.UTF_8));
        System.out.println(sql);
        Statement statement = null;
        long result = 0L;
        try {
            statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                result = rs.getLong("skuSum");
            }
            else{
                return null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public Iterable<Object> get(List<String> keys) {
        String sqlStatement = "select skuSum from sku_window where skuName='%s';";

        List<Object> result = new ArrayList<>();
        for (String key : keys) {
            String sql = new String(String.format(sqlStatement, key).getBytes(StandardCharsets.UTF_8));
            System.out.println(sql);
            Statement statement = null;
            try {
                statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql);
                if (rs.next()) {
                    result.add(rs.getInt("skuSum"));
                    System.out.println(rs.getInt("skuSum"));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public Iterable<String> getAllKeys() {
        String sql = "select skuName from sku_window;";
        System.out.println(sql);
        List<String> result = new ArrayList<>();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                result.add(rs.getString("skuName"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public void put(String key, Object value) {
        String sqlStatement = "insert into sku_window values('%s', %d);";

        String sql = String.format(sqlStatement, key,  value);
        System.out.println(sql);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void putAll(Collection<Entry> entries) {
        entries.forEach(entry->{
                put(entry.key, entry.value);
        });
    }

    @Override
    public void remove(String key) {

        String sqlStatement = "delete from sku_window where skuName='%s';";
        String sql = String.format(sqlStatement, key);
        System.out.println(sql);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void removeAll(Collection<String> keys) {
        keys.forEach(this::remove);
    }

    @Override
    public void shutdown() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
