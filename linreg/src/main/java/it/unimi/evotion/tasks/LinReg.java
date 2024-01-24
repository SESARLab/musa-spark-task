package it.unimi.evotion.tasks;

public class LinReg {
    public static void main(String[] args) throws Exception {
        LinRegTask task = new LinRegTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}