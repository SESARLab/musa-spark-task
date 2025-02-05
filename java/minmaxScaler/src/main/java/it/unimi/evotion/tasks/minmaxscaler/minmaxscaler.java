package it.unimi.evotion.tasks.minmaxscaler;
public class minmaxscaler {
    public static void main(String[] args) throws Exception {
        // usage csvData= constParams= [min= max=] resultPath= labelName
        mmsTask task = new mmsTask();
        task.init(args);
        task.run();
        task.postProcessing();
    }
}