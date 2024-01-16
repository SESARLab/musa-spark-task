package it.unimi.evotion.tasks.kmeans;

import org.apache.log4j.BasicConfigurator;

public class Kmeans {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        kMeansTask task = new kMeansTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}