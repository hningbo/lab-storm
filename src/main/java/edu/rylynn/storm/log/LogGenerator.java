package edu.rylynn.storm.log;

import edu.rylynn.storm.log.entity.Order;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class LogGenerator {
    private static final Logger logger = LogManager.getLogger(LogGenerator.class);

    public void logOrder(Order order) {
        logger.info(order);
    }

    public static void main(String[] args) {
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
