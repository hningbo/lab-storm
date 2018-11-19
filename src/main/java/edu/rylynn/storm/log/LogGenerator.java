package edu.rylynn.storm.log;

import edu.rylynn.storm.log.entity.Order;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LogGenerator {
    private static final Logger logger = LogManager.getLogger(LogGenerator.class);

    public void logSomething(){

    }
    public static void main(String[] args) {
        new LogGenerator().logSomething();
    }
}
