package it.unimi.evotion.tasks.svmModel;

import it.unimi.evotion.tasks.svmModel.SVMModelTask;

public class SVMModel {
    public static void main(String[] args) throws Exception {
        SVMModelTask task = new SVMModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}