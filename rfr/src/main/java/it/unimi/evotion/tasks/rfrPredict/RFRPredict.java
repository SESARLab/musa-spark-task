package it.unimi.evotion.tasks.rfrPredict;

import it.unimi.evotion.tasks.rfrPredict.RFRPredictTask;

public class RFRPredict {
    public static void main(String[] args) throws Exception {
        RFRPredictTask task = new RFRPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}