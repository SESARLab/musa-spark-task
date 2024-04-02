package it.unimi.evotion.tasks.lda_predict;

public class LDAPredict {
    public static void main(String[] args) throws Exception {
        LDAPredictTask task = new LDAPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}