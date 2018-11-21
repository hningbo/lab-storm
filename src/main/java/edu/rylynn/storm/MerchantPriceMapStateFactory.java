package edu.rylynn.storm;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.TransactionalMap;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import java.util.Hashtable;
import java.util.Map;

/**
 * @author rylynn
 * @version 21/11/2018
 * @classname JDBCStateFactory
 * @discription
 */

public class MerchantPriceMapStateFactory implements StateFactory {

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return TransactionalMap.build(new MerchantPriceMapState());
    }
}
