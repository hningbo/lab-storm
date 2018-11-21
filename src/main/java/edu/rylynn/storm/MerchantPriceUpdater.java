package edu.rylynn.storm;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author rylynn
 * @version 21/11/2018
 * @classname MerchantPriceUpdater
 * @discription
 */

//public class MerchantPriceUpdater extends BaseStateUpdater<> {
//    @Override
//    public void updateState(MerchantPriceState state, List<TridentTuple> tuples, TridentCollector collector) {
//        Map<String, Long> map = new HashMap<>();
//        for(TridentTuple tuple : tuples){
//            String marchantName = tuple.getStringByField("marchantName");
//            long totalPrice = tuple.getLongByField("totalPrice");
//            map.put(marchantName, totalPrice);
//        }
//        //state.setBulk(map);
//    }
//}
