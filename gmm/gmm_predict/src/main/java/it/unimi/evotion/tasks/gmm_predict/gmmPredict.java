package it.unimi.evotion.tasks.gmm_predict;

public class gmmPredict {
    public static void main(String[] args) throws Exception {
        gmmpTask task = new gmmpTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}