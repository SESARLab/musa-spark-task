package it.unimi.evotion.tasks;

public class Anova extends AnovaTask {

    public static void main(String[] args) throws Exception {

        AnovaTask task = new AnovaTask();
        task.init(args);
        task.run();
        task.postProcessing();

    }

}
