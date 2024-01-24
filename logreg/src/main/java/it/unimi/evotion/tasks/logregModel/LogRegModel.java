package it.unimi.evotion.tasks.logregModel;

import it.unimi.evotion.tasks.logregModel.LogRegModelTask;

public class LogRegModel {
    public static void main(String[] args) throws Exception {
        LogRegModelTask task = new LogRegModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}