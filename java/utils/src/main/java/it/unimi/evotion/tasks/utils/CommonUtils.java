package it.unimi.evotion.tasks.utils;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CommonUtils {
    private static final Logger logger = LogManager.getLogger(CommonUtils.class);

    public static void sleepIfSystemPropIsSet() throws InterruptedException {
        Optional<Integer> delaySecondsOpt = Optional.ofNullable(
                System.getProperty("custom.spark.driver.delaySecondsBeforeTermination")).map(v -> Integer.valueOf(v));

        if (delaySecondsOpt.isPresent()) {
            int delaySeconds = delaySecondsOpt.get();
            logger.info(String.format("Waiting for %d before terminating the SparkContext...", delaySeconds));
            Thread.sleep(delaySeconds * 1000);
        }
    }
}
