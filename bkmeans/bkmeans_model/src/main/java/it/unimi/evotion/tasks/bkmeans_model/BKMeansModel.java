package it.unimi.evotion.tasks.bkmeans_model;

import it.unimi.evotion.tasks.bkmeans_model.BKMeansModelTask;

public class BKMeansModel {
    public static void main(String[] args) throws Exception {
        BKMeansModelTask task = new BKMeansModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}