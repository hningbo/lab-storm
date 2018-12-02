package edu.rylynn.storm;

import com.mysql.jdbc.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.IBackingMap;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author rylynn
 * @version 21/11/2018
 * @classname JDBCState
 * @discription The grouping fields will be the keys in the state,
 * and the aggregation result will be the values in the state
 */

public class MerchantPriceMapState<T> implements IBackingMap<T> {

    private Logger logger = LogManager.getLogger(MerchantPriceMapState.class);

    private static Connection getConnection() {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://10.113.9.116:3306/order_db";
        String username = "root";
        String password = "root";
        Connection conn = null;
        try {
            Class.forName(driver); //classLoader,加载对应驱动
            conn = (Connection) DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }


    /*
    封装数据库查询的函数，因为事务型操作每次需要查询lastId是否一致，TODO:在这里应该写通过merchant查询(商家名，价格，txid)的代码，方法参数应该是查询条件
    由于persistenceAggregate通常都在groupBy后面，通常用一个Map作为对数据的抽象，下面两个方法的参数即为Map的键和值，keys即为Map的键，表示是以keys作为
    参数做的聚集，value即查询的结果
     */
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        Connection connection = getConnection();
        List<TransactionalValue> result = new ArrayList<>();
        String sql = "select merchantName, totalPrice, txid from order where merchantName =' ";
        for(List<Object> key : keys){
            String merchantName = (String)key.get(0);
            try {
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql + merchantName + "'");
                long txid = rs.getLong("txid");
                String marchantName = rs.getString("marchantName");
                float totalPrice = rs.getFloat("totalPrice");
                result.add(new TransactionalValue(txid, totalPrice))
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        return (List<T>) result;
    }

    /*
    TODO:key是merchant，vals是price和txid
     */
    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        Connection connection = getConnection();
        String sql = "insert into order Values () ";

        try {
            Statement statement = connection.createStatement();
            statement.execute(sql);
        }catch (SQLException e){
            e.printStackTrace();
        }

    }
}
