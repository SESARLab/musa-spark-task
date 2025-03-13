import sys
import os

from pyspark.sql import SparkSession
from spark_tensorflow_distributor import MirroredStrategyRunner

os.environ["CUDA_VISIBLE_DEVICES"] = "-1"


s3_bucket = sys.argv[1]
s3_model_path_prefix = sys.argv[2]

spark = SparkSession.builder.getOrCreate()
sparkContext = spark.sparkContext
num_workers = int(sparkContext.getConf().get('spark.executor.instances'))

print(f"Model training with {num_workers} workers...")

per_worker_batch_size = 64
global_batch_size = per_worker_batch_size * num_workers

s3_endpoint = sparkContext.getConf().get('spark.hadoop.fs.s3a.endpoint')

def save_model_part_on_s3(model_part_path: str, obj_name: str):
    import boto3

    s3_client = boto3.client(service_name='s3', endpoint_url=s3_endpoint)
    s3_client.upload_file(model_part_path, s3_bucket, f'{s3_model_path_prefix}/model/{obj_name}')


# Adapted from https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras
def train():
    import tensorflow as tf
    import uuid
    import json

    BUFFER_SIZE = 10000
    BATCH_SIZE = 64

    def make_datasets():
        (mnist_images, mnist_labels), _ = \
            tf.keras.datasets.mnist.load_data(path=str(uuid.uuid4())+'mnist.npz')

        dataset = tf.data.Dataset.from_tensor_slices((
            tf.cast(mnist_images[..., tf.newaxis] / 255.0, tf.float32),
            tf.cast(mnist_labels, tf.int64))
        )
        dataset = dataset.repeat().shuffle(BUFFER_SIZE).batch(BATCH_SIZE)
        return dataset

    def build_and_compile_cnn_model():
        model = tf.keras.Sequential([
            tf.keras.layers.Conv2D(32, 3, activation='relu', input_shape=(28, 28, 1)),
            tf.keras.layers.MaxPooling2D(),
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(64, activation='relu'),
            tf.keras.layers.Dense(10, activation='softmax'),
        ])
        model.compile(
            loss=tf.keras.losses.sparse_categorical_crossentropy,
            optimizer=tf.keras.optimizers.SGD(learning_rate=0.001),
            metrics=['accuracy'],
        )
        return model

    train_datasets = make_datasets()
    options = tf.data.Options()
    options.experimental_distribute.auto_shard_policy = tf.data.experimental.AutoShardPolicy.DATA
    train_datasets = train_datasets.with_options(options)
    model = build_and_compile_cnn_model()
    model.fit(x=train_datasets, epochs=3, steps_per_epoch=5)
    print('Model training completed')

    tf_config = json.loads(os.environ['TF_CONFIG'])
    
    if tf_config['task']['index'] == 0:
        WEIGHTS_FILE_NAME = 'weights.h5'
        CONFIGS_FILE_NAME = 'configs.json'
        model.save_weights(WEIGHTS_FILE_NAME)
        with open(CONFIGS_FILE_NAME, 'w') as f_configs:
            f_configs.write(model.to_json())

        # save on s3
        save_model_part_on_s3(WEIGHTS_FILE_NAME, WEIGHTS_FILE_NAME)
        save_model_part_on_s3(CONFIGS_FILE_NAME, CONFIGS_FILE_NAME)

        # TODO: cannot save model using model.save, fails - why?
        # model.save(f"./model-{tf_config['task']['index']}.keras")


def train_with_custom_strategy():
    import tensorflow as tf

    communication_options = tf.distribute.experimental.CommunicationOptions(
        implementation=tf.distribute.experimental.CommunicationImplementation.RING)
    # the default one is tf.distribute.experimental.MultiWorkerMirroredStrategy,
    # which is deprecated
    strategy = tf.distribute.MultiWorkerMirroredStrategy(communication_options=communication_options)
    with strategy.scope():
        return train()

MirroredStrategyRunner(num_slots=num_workers, use_gpu=False, use_custom_strategy=True).run(train_with_custom_strategy)
