package edu.rylynn.storm;

import com.mysql.jdbc.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.zookeeper.Transaction;

import java.io.UnsupportedEncodingException;
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

/**
 * TransactionalMap->CachedBatchReadsMap->IBackingMap三层的封装
 * CacheBatchReadsMap的作用是，由于在Trident中常常是以批的方式来进行数据处理，因此设置一个HashMap在内存里来的这批数据缓存下来，
 * 在Map这层抽象中，在框架中我们调用的是TransactionalMap，实际上TransactionalMap再调用CacheBatchReadsMap，CacheBatchReadsMap再
 * 调用我们自己实现的IBackingMap，是这么一层的逻辑关系，因此我们要实现的部分是实际上最底层的部分，拿到的数据也都是
 * TransactionalMap处理好的，也就是加上了txId的数据，因此SQL语句就要以最终插入数据库的来写。事实上也只能以最后插入数据库的
 * 语句来写，框架不会再修改你的JDBC操作。
 *
 */

/**
 * 主要是TransactionalMap中的multiUpdate方法，这个方法完成了保证exactly once的大部分操作。
 *
 */

public class MerchantPriceMapState<T> implements IBackingMap<T> {
    private Connection connection = getConnection();
    private Logger logger = LogManager.getLogger(MerchantPriceMapState.class);

    private static Connection getConnection() {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://10.113.9.116:3306/order_db?useUnicode=true&amp;characterEncoding=UTF-8";
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


    /*
    封装数据库查询的函数，因为事务型操作每次需要查询lastId是否一致，TODO:在这里应该写通过merchant查询(商家名，价格，txid)的代码，方法参数应该是查询条件
    由于persistenceAggregate通常都在groupBy后面，通常用一个Map作为对数据的抽象，下面两个方法的参数即为Map的键和值，keys即为Map的键，表示是以keys作为
    参数做的聚集，value即查询的结果.List<List<>>表示一批数据，每个数据有多个查询的键，即为上层groupBy的结果。

    CacheBatchReadsMap中拿到的就是TransactionalValue类型，然后将其变成一个RetValue类型，这个类型多了一个boolean类型的域，
    可以记录有没有被加入CacheBatchReadsMap的HashMap中，如果这个TransactionalValue已经出现在了缓存中，就说明这个数据是
    由于故障、网络等原因重复发送的，则标记为true，如果还不在TransactionValue中，则说明是第一次出现，则标记为false

    而在TransactionalMap中又将非空的TransactionalValue从RetValue中提取出来
     */
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<TransactionalValue> result = new ArrayList<>();
        String sql = "select totalPrice, txid from order_info where merchantName='%s';";
        for(List<Object> key : keys){
            String merchantName = (String)key.get(0);
            try {
                Statement statement = connection.createStatement();
                String finalSql = String.format(sql, merchantName);
                System.err.println(finalSql);
                ResultSet rs = statement.executeQuery(finalSql);
                if(!rs.next()){
                    result.add(new TransactionalValue(0L, 0.0F));
                }
                else{
                long txid = rs.getLong("txid");
                float totalPrice = rs.getFloat("totalPrice");
                result.add(new TransactionalValue(txid, totalPrice));}
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return (List<T>) result;
    }

    /*
    TODO:key是merchant，vals是price和txid
    这里的price是一个TransactionalValue类型，目前还不知道通过什么传过来的，总之要从value中读取计算的结果以及txid，keys和上面的
    multiGet一样，是从groupBy传过来的类型
    为什么参数vals要设成泛型而不是TransactionalValue型，最早被调用实在TransactionMap中被调用multiPut方法，而这个方法在最初的时候
    TransactionalMap被调用的时候接收的值就是不确定类型的，是经过TransactionalMap封装后才变成TransactionalValue型的。
     */
    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {

        String sql = "insert into order_info(merchantName, totalPrice, txid) values ('%s',%f, %d);";

            for(int i = 0; i<keys.size(); i++){
                List<Object> key = keys.get(i);
                TransactionalValue val = (TransactionalValue)vals.get(i);
                String merchantName = (String)key.get(0);
                float totalPrice = (Float)val.getVal();
                long txid = val.getTxid();
                try {
                    String finalSql = new String(String.format(sql, merchantName, totalPrice, txid).getBytes("utf-8"), "utf-8");
                    System.err.println(finalSql);
                    Statement statement = connection.createStatement();
                    statement.execute(finalSql);
                } catch (UnsupportedEncodingException | SQLException e) {
                    e.printStackTrace();
                }
            }
    }
}
