package it.unimi.evotion.tasks.rfrModel;

import it.unimi.evotion.tasks.rfrModel.RFRModelTask;

public class RFRModel {
    public static void main(String[] args) throws Exception {
        RFRModelTask task = new RFRModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}