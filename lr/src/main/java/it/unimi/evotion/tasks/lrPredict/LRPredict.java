package it.unimi.evotion.tasks.lrPredict;

import it.unimi.evotion.tasks.lrPredict.LRPredictTask;

public class LRPredict {
    public static void main(String[] args) throws Exception {
        LRPredictTask task = new LRPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}