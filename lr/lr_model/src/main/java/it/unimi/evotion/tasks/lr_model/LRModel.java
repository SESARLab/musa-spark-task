package it.unimi.evotion.tasks.lr_model;

public class LRModel {
    public static void main(String[] args) throws Exception {
        LRModelTask task = new LRModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}