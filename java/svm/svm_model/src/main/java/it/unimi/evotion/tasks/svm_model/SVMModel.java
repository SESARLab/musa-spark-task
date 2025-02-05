package it.unimi.evotion.tasks.svm_model;

public class SVMModel {
    public static void main(String[] args) throws Exception {
        SVMModelTask task = new SVMModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}