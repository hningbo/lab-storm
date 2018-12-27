package edu.rylynn.storm.drpc;

import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class MyDRPCClient {
    public static void main(String[] args) throws Exception {
        Map cfg = Utils.readDefaultConfig();
        DRPCClient client = new DRPCClient(cfg, "master", 7001);
        System.out.println(client.execute("skuName", "黑色连衣裙"));

    }
}
