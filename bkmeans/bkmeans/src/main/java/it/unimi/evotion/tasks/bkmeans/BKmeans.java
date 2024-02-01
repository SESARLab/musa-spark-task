package it.unimi.evotion.tasks.bkmeans;

import it.unimi.evotion.tasks.bkmeans.bkMeansTask;

public class BKmeans {
    public static void main(String[] args) throws Exception {
        bkMeansTask task = new bkMeansTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}