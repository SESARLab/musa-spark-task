package it.unimi.evotion.tasks.ldaPredict;

import it.unimi.evotion.tasks.ldaPredict.LDAPredictTask;

public class LDAPredict {
    public static void main(String[] args) throws Exception {
        LDAPredictTask task = new LDAPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}