package it.unimi.evotion.tasks.svm;

public class Svm {
    public static void main(String[] args) throws Exception {
        SvmTask task = new SvmTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}