/opt/spark/bin/spark-submit \
    --class it.unimi.evotion.tasks.svm.Svm \
    jar_path \
    dataset_path \
    feature1,feature2,feature3 \
    origLabel \
    10 \
    0.1 \
    output_path