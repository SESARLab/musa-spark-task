package it.unimi.evotion.tasks.lda;

import it.unimi.evotion.tasks.lda.ldaTask;

public class LDA {
    public static void main(String[] args) throws Exception {
        ldaTask task = new ldaTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}