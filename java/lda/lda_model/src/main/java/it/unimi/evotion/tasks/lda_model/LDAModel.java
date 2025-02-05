package it.unimi.evotion.tasks.lda_model;

public class LDAModel {
    public static void main(String[] args) throws Exception {
        LDAModelTask task = new LDAModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}