#! /bin/bash

echo "Testing $1..."
if [ $# -lt 2 ]
  then
    echo "Usage: ./launchDockerKmeans.sh <task> <dataset> [PARAMS]"
    exit 1
fi

task=$1
ds=$2
shift 2
echo "Arguments: $@"

modelDataArg="/../foo"
for arg in "$@"
do
  if [[ $arg == modelData=* ]]; then
    modelDataArg=${arg#modelData=}
    break
  fi
done

docker build --build-arg jarpath=${task%_*}/out/artifacts/${task}_jar/ --build-arg dataset=${ds} --build-arg modeldata=${task%_*}${modelDataArg}/ --build-arg modeldir=${modelDataArg} --no-cache --progress=plain -t ${task}-container-image .

docker run -it ${task}-container-image /usr/lib/jvm/java-1.8.0-amazon-corretto/bin/java -jar /jars/${task}.jar "$@"
