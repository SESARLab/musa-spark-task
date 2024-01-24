package it.unimi.evotion.tasks.kmeansPredict;

import it.unimi.evotion.tasks.kmeansPredict.KMeansPredictTask;

public class KMeansPredict {
    public static void main(String[] args) throws Exception {
        KMeansPredictTask task = new KMeansPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}