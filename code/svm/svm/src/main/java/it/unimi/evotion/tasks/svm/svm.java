package it.unimi.evotion.tasks.svm;
public class svm {
    public static void main(String[] args) throws Exception {
        svmTask task = new svmTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}