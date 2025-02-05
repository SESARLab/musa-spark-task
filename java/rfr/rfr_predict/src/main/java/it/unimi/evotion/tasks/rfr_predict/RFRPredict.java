package it.unimi.evotion.tasks.rfr_predict;

import it.unimi.evotion.tasks.rfr_predict.RFRPredictTask;

public class RFRPredict {
    public static void main(String[] args) throws Exception {
        RFRPredictTask task = new RFRPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}