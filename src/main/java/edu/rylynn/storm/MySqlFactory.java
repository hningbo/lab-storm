package edu.rylynn.storm;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.map.TransactionalMap;

import java.util.Map;

public class MySqlFactory implements StateFactory {
    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i1) {
        MySqlState state = new MySqlState();
        return TransactionalMap.build(state);
    }
}
