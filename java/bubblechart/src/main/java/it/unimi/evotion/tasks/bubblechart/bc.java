package it.unimi.evotion.tasks.bubblechart;
public class bc {
    public static void main(String[] args) throws Exception {
        bcTask task = new bcTask();
        task.init(args);
        task.run();
   //     task.postProcessing();
    }
}