package it.unimi.evotion.tasks.lrModel;

import it.unimi.evotion.tasks.lrModel.LRModelTask;

public class LRModel {
    public static void main(String[] args) throws Exception {
        LRModelTask task = new LRModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}