package edu.rylynn.storm.drpc;

import org.apache.storm.trident.windowing.WindowsStore;
import org.apache.storm.trident.windowing.WindowsStoreFactory;

import java.util.Map;

/**
 * @author rylynn
 */

public class MySqlWindowsStoreFactory implements WindowsStoreFactory {
    @Override
    public WindowsStore create(Map stormConf) {
        return new MySqlWindowsStore();
    }
}
