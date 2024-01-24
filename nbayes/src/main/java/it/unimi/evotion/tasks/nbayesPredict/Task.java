package it.unimi.evotion.tasks.nbayesPredict;

public interface Task {

    void init(Object... args) throws Exception;

    void run(Object... params) throws Exception;

    void postProcessing(Object... params) throws Exception;
}
