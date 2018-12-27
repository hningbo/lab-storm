package edu.rylynn.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.config.SlidingDurationWindow;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author rylynn
 */

public class TridentWindow {
    private static StormTopology buildWindowTopo(LocalDRPC drpc) {
        TridentTopology topology = new TridentTopology();


        //TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(ZK_HOST, TOPIC, SPOUT_ID);
        FixedBatchSpout fixedBatchSpout = new FixedBatchSpout(new Fields("order"), 3,
                new Values("2018-11-21 19:38:29 [bigdata.experiment.storm.OrdersLogGenerator.main()] INFO  bigdata.experiment.storm.OrdersLogGenerator - orderNumber: 105711542800309096 | orderDate: 2018-11-21 19:38:29 | paymentNumber: Wechat-03534891 | paymentDate: 2018-11-21 19:38:29 | merchantName: Oracle | sku: [ skuName: 棕色衬衫 skuNum: 3 skuCode: a24rz32gxm skuPrice: 699.0 totalSkuPrice: 2097.0; skuName: 人字拖鞋 skuNum: 3 skuCode: 464u2ryfa5 skuPrice: 899.0 totalSkuPrice: 2697.0; skuName: 塑身牛仔裤 skuNum: 1 skuCode: sror6630at skuPrice: 299.0 totalSkuPrice: 299.0; ] | price: [ totalPrice: 5093.0 discount: 100.0 paymentPrice: 4993.0 ]"),
                new Values("2018-11-21 19:38:27 [bigdata.experiment.storm.OrdersLogGenerator.main()] INFO  bigdata.experiment.storm.OrdersLogGenerator - orderNumber: 488741542800307093 | orderDate: 2018-11-21 19:38:27 | paymentNumber: Paypal-92509579 | paymentDate: 2018-11-21 19:38:27 | merchantName: 守望先峰 | sku: [ skuName: 灰色连衣裙 skuNum: 2 skuCode: 668kmxca1w skuPrice: 1000.0 totalSkuPrice: 2000.0; skuName: 朋克卫衣 skuNum: 2 skuCode: 14rqnikr6n skuPrice: 699.0 totalSkuPrice: 1398.0; skuName: 人字拖鞋 skuNum: 2 skuCode: 22v4drxrgq skuPrice: 699.0 totalSkuPrice: 1398.0; ] | price: [ totalPrice: 4796.0 discount: 100.0 paymentPrice: 4696.0 ]"),
                new Values("2018-11-21 19:38:28 [bigdata.experiment.storm.OrdersLogGenerator.main()] INFO  bigdata.experiment.storm.OrdersLogGenerator - orderNumber: 951841542800308094 | orderDate: 2018-11-21 19:38:28 | paymentNumber: Wechat-40975762 | paymentDate: 2018-11-21 19:38:28 | merchantName: 跑男 | sku: [ skuName: 沙滩拖鞋 skuNum: 1 skuCode: mn3u1zliwv skuPrice: 1000.0 totalSkuPrice: 1000.0; skuName: 灰色连衣裙 skuNum: 1 skuCode: 9p9lxv1esz skuPrice: 299.0 totalSkuPrice: 299.0; skuName: 朋克卫衣 skuNum: 1 skuCode: 3usj179ivs skuPrice: 2000.0 totalSkuPrice: 2000.0; ] | price: [ totalPrice: 3299.0 discount: 100.0 paymentPrice: 3199.0 ]"),
                new Values("2018-11-21 19:38:31 [bigdata.experiment.storm.OrdersLogGenerator.main()] INFO  bigdata.experiment.storm.OrdersLogGenerator - orderNumber: 020431542800311098 | orderDate: 2018-11-21 19:38:31 | paymentNumber: Wechat-39613058 | paymentDate: 2018-11-21 19:38:31 | merchantName: 优衣库 | sku: [ skuName: 朋克卫衣 skuNum: 3 skuCode: xwirhfpvjt skuPrice: 1000.0 totalSkuPrice: 3000.0; skuName: 圆脚牛仔裤 skuNum: 2 skuCode: o2swalx6vy skuPrice: 899.0 totalSkuPrice: 1798.0; skuName: 朋克卫衣 skuNum: 2 skuCode: 25ekcu011n skuPrice: 299.0 totalSkuPrice: 598.0; ] | price: [ totalPrice: 5396.0 discount: 10.0 paymentPrice: 5386.0 ]"),
                new Values("2018-11-21 19:38:30 [bigdata.experiment.storm.OrdersLogGenerator.main()] INFO  bigdata.experiment.storm.OrdersLogGenerator - orderNumber: 461481542800310097 | orderDate: 2018-11-21 19:38:30 | paymentNumber: Alipay-57404294 | paymentDate: 2018-11-21 19:38:30 | merchantName: 暴雪公司 | sku: [ skuName: 黑色连衣裙 skuNum: 3 skuCode: 05opwzo8kv skuPrice: 1000.0 totalSkuPrice: 3000.0; skuName: 黑色连衣裙 skuNum: 1 skuCode: xrzmqija9s skuPrice: 1000.0 totalSkuPrice: 1000.0; skuName: 黑色连衣裙 skuNum: 1 skuCode: f3vbaeu9c2 skuPrice: 399.0 totalSkuPrice: 399.0; ] | price: [ totalPrice: 4399.0 discount: 100.0 paymentPrice: 4299.0 ]"));
        fixedBatchSpout.setCycle(true);

        WindowConfig slidingWindow = SlidingDurationWindow.of(BaseWindowedBolt.Duration.seconds(60), BaseWindowedBolt.Duration.seconds(5));

        TridentState state = topology.newStream("kafkaSpout", fixedBatchSpout).
                each(new Fields("order"), new SplitVolumn(), new Fields("skuName", "skuNum")).
                window(slidingWindow, new Fields("skuName", "skuNum"), new SumAggregator(), new Fields("skuName", "skuNum")).
                groupBy(new Fields("skuName")).
                persistentAggregate(new MemoryMapState.Factory(), new Fields("skuName", "skuNum"), new LongSum(), new Fields("skuTotalNum")).
                parallelismHint(8);

        topology.newDRPCStream("args", drpc).
                each(new Fields("args"), new ParamsParser(), new Fields("skuName")).
                stateQuery(state, new Fields("skuName"), new MapGet(), new Fields("skuTotalNum"));

        return topology.build();
    }


    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.setMaxSpoutPending(20);
        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();


        StormTopology topology = buildWindowTopo(drpc);
        cluster.submitTopology("tridentWindow", config, topology);

        Thread.sleep(2000);
        for (int i = 0; i < 100; i++) {
            System.out.println("DRPC Result : " + drpc.execute("args", "黑色连衣裙"));
            Thread.sleep(1000);
        }

    }


    private static class SumAggregator extends BaseAggregator<HashMap<String, Long>> {
        @Override
        public HashMap<String, Long> init(Object batchId, TridentCollector collector) {
            return new HashMap<>();
        }

        @Override
        public void aggregate(HashMap<String, Long> val, TridentTuple tuple, TridentCollector collector) {
            String skuName = tuple.getStringByField("skuName");
            long skuNum = tuple.getLongByField("skuNum");
            if (val.containsKey(skuName)) {
                long skuLastNum = val.get(skuName);
                val.put(skuName, skuLastNum + skuNum);
            } else {
                val.put(skuName, skuNum);
            }
        }

        @Override
        public void complete(HashMap<String, Long> val, TridentCollector collector) {

            System.err.println("-------------------------------------");
            System.err.println(new Date());
            for (Map.Entry entry : val.entrySet()) {
                collector.emit(new Values(entry.getKey(), entry.getValue()));
                System.err.println(entry.getKey() + " : " + entry.getValue());
            }

            System.err.println("-------------------------------------");
        }
    }

    private static class LongSum implements CombinerAggregator<Long> {

        @Override
        public Long init(TridentTuple tuple) {
            System.out.println(tuple);
            return tuple.getLong(1);
        }

        @Override
        public Long combine(Long val1, Long val2) {
            return val2;
        }

        @Override
        public Long zero() {
            return 0L;
        }
    }

    private static class SplitVolumn extends BaseFunction {
        /*With format
         * orderNumber: XX | orderDate: XX | paymentNumber: XX |
         *  paymentDate: XX | merchantName: XX |
         *  sku: [ skuName: XX skuNum: XX skuCode: XX skuPrice: XX totalSkuPrice: XX;skuName: XX skuNum: XX skuCode: XX skuPrice: XX totalSkuPrice: XX;] |
         *  price: [ totalPrice: XX discount: XX paymentPrice: XX ]
         */

        /**
         * output: field1:MerchantName
         * field2: totalPrice
         */
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String order = tuple.getStringByField("order");
            String skuName = order.split("skuName: ")[1].split("skuNum")[0].trim();
            long skuNum = Long.parseLong(order.split("skuNum:")[1].split("skuCode")[0].trim());
            collector.emit(new Values(skuName, skuNum));
        }
    }

    private static class ParamsParser extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String params = tuple.getString(0);
            collector.emit(Stream.of(params.split(",")).collect(Collectors.toList()));
        }
    }

}
