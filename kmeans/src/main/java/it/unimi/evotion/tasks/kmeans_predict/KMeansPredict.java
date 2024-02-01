package it.unimi.evotion.tasks.kmeans_predict;

public class KMeansPredict {
    public static void main(String[] args) throws Exception {
        KMeansPredictTask task = new KMeansPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}