package it.unimi.evotion.tasks.rfcPredict;

import it.unimi.evotion.tasks.rfcPredict.RFCPredictTask;

public class RFCPredict {
    public static void main(String[] args) throws Exception {
        RFCPredictTask task = new RFCPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}