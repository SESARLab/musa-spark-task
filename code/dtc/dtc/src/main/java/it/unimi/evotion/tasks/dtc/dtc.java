package it.unimi.evotion.tasks.dtc;

import it.unimi.evotion.tasks.dtc.dtcTask;

public class dtc {
    public static void main(String[] args) throws Exception {
        dtcTask task = new dtcTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}