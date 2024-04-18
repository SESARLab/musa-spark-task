package it.unimi.evotion.tasks.logreg_model;

public class LogRegModel {
    public static void main(String[] args) throws Exception {
        LogRegModelTask task = new LogRegModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}