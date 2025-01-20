/opt/spark/bin/spark-submit \
    --class it.unimi.evotion.tasks.lr.LReg \
    jar_path \
    dataset_path \
    HA_USAGE \
    HA_USAGE_SCALED \
    10 \
    5 \
    0.8 \
    output_path