package it.unimi.evotion.tasks.dtc_predict;

public class DTCPredict {
    public static void main(String[] args) throws Exception {
        DTCPredictTask task = new DTCPredictTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}