/opt/spark/bin/spark-submit \
    --class it.unimi.evotion.tasks.Anova \
    jarpath/ \
    csvData=datasetpath.csv \
    labelName=Score \
    resultPath=path