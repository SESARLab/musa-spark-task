package it.unimi.evotion.tasks.svm_predict;

import it.unimi.evotion.tasks.svm_predict.SVMPredictTask;

public class SVMPredict {
    public static void main(String[] args) throws Exception {
        SVMPredictTask task = new SVMPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}