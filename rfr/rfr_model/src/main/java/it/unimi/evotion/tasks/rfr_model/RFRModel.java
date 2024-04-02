package it.unimi.evotion.tasks.rfr_model;

import it.unimi.evotion.tasks.rfr_model.RFRModelTask;

public class RFRModel {
    public static void main(String[] args) throws Exception {
        RFRModelTask task = new RFRModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}