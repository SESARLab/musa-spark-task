package it.unimi.evotion.tasks.ldaModel;

import it.unimi.evotion.tasks.ldaModel.LDAModelTask;

public class LDAModel {
    public static void main(String[] args) throws Exception {
        LDAModelTask task = new LDAModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}