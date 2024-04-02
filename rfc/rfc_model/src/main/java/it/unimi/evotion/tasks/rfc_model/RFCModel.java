package it.unimi.evotion.tasks.rfc_model;

import it.unimi.evotion.tasks.rfc_model.RFCModelTask;

public class RFCModel {
    public static void main(String[] args) throws Exception {
        RFCModelTask task = new RFCModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}