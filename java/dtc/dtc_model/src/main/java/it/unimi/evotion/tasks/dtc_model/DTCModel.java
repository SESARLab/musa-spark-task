package it.unimi.evotion.tasks.dtc_model;

public class DTCModel {
    public static void main(String[] args) throws Exception {
        DTCModelTask task = new DTCModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}