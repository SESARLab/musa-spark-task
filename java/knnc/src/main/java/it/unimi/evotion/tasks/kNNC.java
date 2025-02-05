package it.unimi.evotion.tasks;
public class kNNC {
    public static void main(String[] args) throws Exception {
        kNNCTask task = new kNNCTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}