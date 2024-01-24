package it.unimi.evotion.tasks.logregPredict;

import it.unimi.evotion.tasks.logregPredict.LogRegPredictTask;

public class LogRegPredict {
    public static void main(String[] args) throws Exception {
        LogRegPredictTask task = new LogRegPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}