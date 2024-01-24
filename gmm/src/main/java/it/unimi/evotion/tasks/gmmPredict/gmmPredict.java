package it.unimi.evotion.tasks.gmmPredict;

import it.unimi.evotion.tasks.gmmPredict.gmmpTask;

public class gmmPredict {
    public static void main(String[] args) throws Exception {
        gmmpTask task = new gmmpTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}