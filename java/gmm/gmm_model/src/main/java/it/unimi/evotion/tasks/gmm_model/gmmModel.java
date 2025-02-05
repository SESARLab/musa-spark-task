package it.unimi.evotion.tasks.gmm_model;

public class gmmModel {
    public static void main(String[] args) throws Exception {
        gmmmTask task = new gmmmTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}