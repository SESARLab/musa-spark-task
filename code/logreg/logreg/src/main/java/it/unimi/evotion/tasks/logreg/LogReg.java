package it.unimi.evotion.tasks.logreg;
public class LogReg {
    public static void main(String[] args) throws Exception {
        LogRegTask task = new LogRegTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}