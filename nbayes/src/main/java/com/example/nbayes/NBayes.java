package com.example.nbayes;
public class NBayes {
    public static void main(String[] args) throws Exception {
        nBayesTask task = new nBayesTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}