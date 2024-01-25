package it.unimi.evotion.tasks;

public class RF {
    public static void main(String[] args) throws Exception {
        rfTask task = new rfTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}