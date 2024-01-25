package it.unimi.evotion.tasks;

public class pca {
    public static void main(String[] args) throws Exception {
        pcaTask task = new pcaTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}