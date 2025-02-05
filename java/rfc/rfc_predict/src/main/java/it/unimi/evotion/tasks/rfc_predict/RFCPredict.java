package it.unimi.evotion.tasks.rfc_predict;

import it.unimi.evotion.tasks.rfc_predict.RFCPredictTask;

public class RFCPredict {
    public static void main(String[] args) throws Exception {
        RFCPredictTask task = new RFCPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}