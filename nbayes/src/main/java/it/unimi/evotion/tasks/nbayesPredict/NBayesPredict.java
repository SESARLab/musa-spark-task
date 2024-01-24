package it.unimi.evotion.tasks.nbayesPredict;

import it.unimi.evotion.tasks.nbayesPredict.NBayesPredictTask;

public class NBayesPredict {
    public static void main(String[] args) throws Exception {
        NBayesPredictTask task = new NBayesPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}