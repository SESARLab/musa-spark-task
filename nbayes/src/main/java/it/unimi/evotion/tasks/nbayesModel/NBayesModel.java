package it.unimi.evotion.tasks.nbayesModel;

import it.unimi.evotion.tasks.nbayesModel.NBayesModelTask;

public class NBayesModel {
    public static void main(String[] args) throws Exception {
        NBayesModelTask task = new NBayesModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}