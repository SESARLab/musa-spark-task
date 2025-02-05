package it.unimi.evotion.tasks.kmeans_model;

public class KMeansModel {
    public static void main(String[] args) throws Exception {
        KMeansModelTask task = new KMeansModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}