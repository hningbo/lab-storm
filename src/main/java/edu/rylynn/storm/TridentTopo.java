package edu.rylynn.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentTopo {
    private static final BrokerHosts ZK_HOST = new ZkHosts("10.113.9.116:2181");
    private static final String TOPIC = "flumeTopic";
    private static final String SPOUT_ID = "kafkaSpout";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        TridentTopology tridentTopology = new TridentTopo().buildTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("orderPriceCount", config, tridentTopology.build());
        //StormSubmitter.submitTopology("orderPriceCount", config, tridentTopology.build());
    }

    private TridentTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(ZK_HOST, TOPIC, SPOUT_ID);
        TransactionalTridentKafkaSpout tridentKafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);
        topology.newStream("kafkaSpout", tridentKafkaSpout).
                each(new Fields("order"), new getMerchantPrice(), new Fields("merchantName", "totalPrice")).
                groupBy(new Fields("merchantName")).
                persistentAggregate(new MemoryMapState.Factory(),new Count(), new Fields("totalPrice")).
                parallelismHint(6);

        return topology;
    }

    private class getMerchantPrice extends BaseFunction {

        /*With format
         * orderNumber: XX | orderDate: XX | paymentNumber: XX |
         *  paymentDate: XX | merchantName: XX |
         *  sku: [ skuName: XX skuNum: XX skuCode: XX skuPrice: XX totalSkuPrice: XX;skuName: XX skuNum: XX skuCode: XX skuPrice: XX totalSkuPrice: XX;] |
         *  price: [ totalPrice: XX discount: XX paymentPrice: XX ]
         */

        /*
        output:field1:MerchantName
                field2 totalPrice
         */
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String order = tridentTuple.getStringByField("order");
            String merchantName = order.split("merchantName:")[1].split("|")[0].trim();
            String totalPrice = order.split("totalPrice")[1].split("discount")[0].trim();
            tridentCollector.emit(new Values(merchantName, totalPrice));
        }
    }
}
