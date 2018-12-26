package edu.rylynn.storm.drpc;

import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;

public class DRPCStreaming {
    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        LocalDRPC localDRPC = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();


    }


}
