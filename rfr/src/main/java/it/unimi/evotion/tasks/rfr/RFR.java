package it.unimi.evotion.tasks.rfr;

import it.unimi.evotion.tasks.rfr.rfrTask;

public class RFR {
    public static void main(String[] args) throws Exception {
        rfrTask task = new rfrTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}