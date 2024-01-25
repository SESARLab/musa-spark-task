package it.unimi.evotion.tasks.svmPredict;

import it.unimi.evotion.tasks.svmPredict.SVMPredictTask;

public class SVMPredict {
    public static void main(String[] args) throws Exception {
        SVMPredictTask task = new SVMPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}