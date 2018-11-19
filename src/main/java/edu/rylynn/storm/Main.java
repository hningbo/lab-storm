package edu.rylynn.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.trident.TridentTopology;

public class Main {

    private static final String KAFKA_TOPIC = "flumeTopic";
    private static final String ZK_HOST = "master:2181";

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TridentTopology topology = new TridentTopology();
        BrokerHosts hosts = new ZkHosts(ZK_HOST);
        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(hosts, KAFKA_TOPIC, "1");
        TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);
        topology.newStream("name", spout);
        Config config = new Config();
        StormSubmitter.submitTopology("XXX", config, topology.build());
        //SpoutConfig config = new SpoutConfig(brokerHosts,KAFKA_TOPIC);


    }
}
