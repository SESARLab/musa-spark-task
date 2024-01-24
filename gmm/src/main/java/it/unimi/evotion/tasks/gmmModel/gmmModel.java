package it.unimi.evotion.tasks.gmmModel;

import it.unimi.evotion.tasks.gmmModel.gmmmTask;

public class gmmModel {
    public static void main(String[] args) throws Exception {
        gmmmTask task = new gmmmTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}