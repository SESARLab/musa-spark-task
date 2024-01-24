package it.unimi.evotion.tasks.lr;

import it.unimi.evotion.tasks.lr.LRegTask;

public class LReg {
    public static void main(String[] args) throws Exception {
        LRegTask task = new LRegTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}