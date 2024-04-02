package it.unimi.evotion.tasks.logreg_predict;

public class LogRegPredict {
    public static void main(String[] args) throws Exception {
        LogRegPredictTask task = new LogRegPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}