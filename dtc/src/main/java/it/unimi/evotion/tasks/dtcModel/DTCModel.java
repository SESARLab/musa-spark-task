package it.unimi.evotion.tasks.dtcModel;

import it.unimi.evotion.tasks.dtcModel.DTCModelTask;

public class DTCModel {
    public static void main(String[] args) throws Exception {
        DTCModelTask task = new DTCModelTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}