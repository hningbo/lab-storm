package edu.rylynn.storm.transaction;

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
import org.apache.storm.shade.org.apache.commons.fileupload.util.Streams;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class TridentTopo {
    private static final BrokerHosts ZK_HOST = new ZkHosts("10.113.9.116:2181");
    private static final String TOPIC = "flumeTopic";
    private static final String SPOUT_ID = "kafkaSpout";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        new TridentTopo();
        TridentTopology tridentTopology = buildTopology();
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("orderPriceCount", config, tridentTopology.build());
        StormSubmitter.submitTopology("orderPriceCount", config, tridentTopology.build());
    }

    private static TridentTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(ZK_HOST, TOPIC);
//        FixedBatchSpout fixedBatchSpout = new FixedBatchSpout(new Fields("order"), 3,
//                new Values("2018-11-21 19:38:29 [bigdata.experiment.storm.OrdersLogGenerator.main()] INFO  bigdata.experiment.storm.OrdersLogGenerator - orderNumber: 105711542800309096 | orderDate: 2018-11-21 19:38:29 | paymentNumber: Wechat-03534891 | paymentDate: 2018-11-21 19:38:29 | merchantName: Oracle | sku: [ skuName: 棕色衬衫 skuNum: 3 skuCode: a24rz32gxm skuPrice: 699.0 totalSkuPrice: 2097.0; skuName: 人字拖鞋 skuNum: 3 skuCode: 464u2ryfa5 skuPrice: 899.0 totalSkuPrice: 2697.0; skuName: 塑身牛仔裤 skuNum: 1 skuCode: sror6630at skuPrice: 299.0 totalSkuPrice: 299.0; ] | price: [ totalPrice: 5093.0 discount: 100.0 paymentPrice: 4993.0 ]"),
//                new Values("2018-11-21 19:38:29 [bigdata.experiment.storm.OrdersLogGenerator.main()] INFO  bigdata.experiment.storm.OrdersLogGenerator - orderNumber: 105711542800309096 | orderDate: 2018-11-21 19:38:29 | paymentNumber: Wechat-03534891 | paymentDate: 2018-11-21 19:38:29 | merchantName: Oracle | sku: [ skuName: 棕色衬衫 skuNum: 3 skuCode: a24rz32gxm skuPrice: 699.0 totalSkuPrice: 2097.0; skuName: 人字拖鞋 skuNum: 3 skuCode: 464u2ryfa5 skuPrice: 899.0 totalSkuPrice: 2697.0; skuName: 塑身牛仔裤 skuNum: 1 skuCode: sror6630at skuPrice: 299.0 totalSkuPrice: 299.0; ] | price: [ totalPrice: 5093.0 discount: 100.0 paymentPrice: 4993.0 ]"),
//                new Values("2018-11-21 19:38:27 [bigdata.experiment.storm.OrdersLogGenerator.main()] INFO  bigdata.experiment.storm.OrdersLogGenerator - orderNumber: 488741542800307093 | orderDate: 2018-11-21 19:38:27 | paymentNumber: Paypal-92509579 | paymentDate: 2018-11-21 19:38:27 | merchantName: 守望先峰 | sku: [ skuName: 灰色连衣裙 skuNum: 2 skuCode: 668kmxca1w skuPrice: 1000.0 totalSkuPrice: 2000.0; skuName: 朋克卫衣 skuNum: 2 skuCode: 14rqnikr6n skuPrice: 699.0 totalSkuPrice: 1398.0; skuName: 人字拖鞋 skuNum: 2 skuCode: 22v4drxrgq skuPrice: 699.0 totalSkuPrice: 1398.0; ] | price: [ totalPrice: 4796.0 discount: 100.0 paymentPrice: 4696.0 ]"),
//                new Values("2018-11-21 19:38:28 [bigdata.experiment.storm.OrdersLogGenerator.main()] INFO  bigdata.experiment.storm.OrdersLogGenerator - orderNumber: 951841542800308094 | orderDate: 2018-11-21 19:38:28 | paymentNumber: Wechat-40975762 | paymentDate: 2018-11-21 19:38:28 | merchantName: 跑男 | sku: [ skuName: 沙滩拖鞋 skuNum: 1 skuCode: mn3u1zliwv skuPrice: 1000.0 totalSkuPrice: 1000.0; skuName: 灰色连衣裙 skuNum: 1 skuCode: 9p9lxv1esz skuPrice: 299.0 totalSkuPrice: 299.0; skuName: 朋克卫衣 skuNum: 1 skuCode: 3usj179ivs skuPrice: 2000.0 totalSkuPrice: 2000.0; ] | price: [ totalPrice: 3299.0 discount: 100.0 paymentPrice: 3199.0 ]"),
//                new Values("2018-11-21 19:38:31 [bigdata.experiment.storm.OrdersLogGenerator.main()] INFO  bigdata.experiment.storm.OrdersLogGenerator - orderNumber: 020431542800311098 | orderDate: 2018-11-21 19:38:31 | paymentNumber: Wechat-39613058 | paymentDate: 2018-11-21 19:38:31 | merchantName: 优衣库 | sku: [ skuName: 朋克卫衣 skuNum: 3 skuCode: xwirhfpvjt skuPrice: 1000.0 totalSkuPrice: 3000.0; skuName: 圆脚牛仔裤 skuNum: 2 skuCode: o2swalx6vy skuPrice: 899.0 totalSkuPrice: 1798.0; skuName: 朋克卫衣 skuNum: 2 skuCode: 25ekcu011n skuPrice: 299.0 totalSkuPrice: 598.0; ] | price: [ totalPrice: 5396.0 discount: 10.0 paymentPrice: 5386.0 ]"),
//                new Values("2018-11-21 19:38:30 [bigdata.experiment.storm.OrdersLogGenerator.main()] INFO  bigdata.experiment.storm.OrdersLogGenerator - orderNumber: 461481542800310097 | orderDate: 2018-11-21 19:38:30 | paymentNumber: Alipay-57404294 | paymentDate: 2018-11-21 19:38:30 | merchantName: 暴雪公司 | sku: [ skuName: 黑色连衣裙 skuNum: 3 skuCode: 05opwzo8kv skuPrice: 1000.0 totalSkuPrice: 3000.0; skuName: 黑色连衣裙 skuNum: 1 skuCode: xrzmqija9s skuPrice: 1000.0 totalSkuPrice: 1000.0; skuName: 黑色连衣裙 skuNum: 1 skuCode: f3vbaeu9c2 skuPrice: 399.0 totalSkuPrice: 399.0; ] | price: [ totalPrice: 4399.0 discount: 100.0 paymentPrice: 4299.0 ]"));
//        fixedBatchSpout.setCycle(true);


        TransactionalTridentKafkaSpout tridentKafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);
        topology.newStream("kafkaSpout", tridentKafkaSpout).
                shuffle().
                each(new Fields("bytes"), new SplitFunction(), new Fields("order")).
                //这里对订单进行分割，得到[订单名，价格]的格式
                each(new Fields("order"), new getMerchantPrice(), new Fields("merchantName", "price")).
                groupBy(new Fields("merchantName")).
                persistentAggregate(new MerchantPriceMapStateFactory(), new Fields("price"), new getTotalPrice(), new Fields("totalPrice")).
                parallelismHint(4);
        return topology;
    }


    private static class getTotalPrice implements ReducerAggregator<Float> {
        @Override
        public Float init() {
            return 0.0f;
        }

        @Override
        public Float reduce(Float curr, TridentTuple tuple) {

            return curr + tuple.getFloatByField("price");
        }
    }

    private static class getMerchantPrice extends BaseFunction {

        /*With format
         * orderNumber: XX | orderDate: XX | paymentNumber: XX |
         *  paymentDate: XX | merchantName: XX |
         *  sku: [ skuName: XX skuNum: XX skuCode: XX skuPrice: XX totalSkuPrice: XX;skuName: XX skuNum: XX skuCode: XX skuPrice: XX totalSkuPrice: XX;] |
         *  price: [ totalPrice: XX discount: XX paymentPrice: XX ]
         */

        /*
         *      output:field1:MerchantName
         *   field2 totalPrice
         */
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String order = tridentTuple.getStringByField("order");

            String merchantName = order.split("merchantName:")[1].split("\\|")[0].trim();
            float totalPrice = Float.parseFloat(order.split("totalPrice:")[1].split("discount")[0].trim());
            System.err.println(merchantName + " " + totalPrice);
            tridentCollector.emit(new Values(merchantName, totalPrice));

        }
    }

    private static class SplitFunction extends BaseFunction{

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {

            //kafka传来的是byte数组，要转成字符串
            String orders = new String(tuple.getBinary(0), StandardCharsets.UTF_8);
            collector.emit(new Values(orders));
            //Stream.of(array).map(Values::new).forEach(collector::emit);
        }
    }
}
