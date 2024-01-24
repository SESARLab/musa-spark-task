package it.unimi.evotion.tasks.bkmeansPredict;

import it.unimi.evotion.tasks.bkmeansPredict.BKMeansPredictTask;

public class BKMeansPredict {
    public static void main(String[] args) throws Exception {
        BKMeansPredictTask task = new BKMeansPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}