package it.unimi.evotion.tasks;

import it.unimi.evotion.tasks.anova.AnovaResult;

public interface IAnova {

    void init();

    void evaluate();

    AnovaResult result();
}
