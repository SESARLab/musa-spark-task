#! /bin/zsh

if [ $# -lt 2 ]
  then
    echo -e "\e[1;33mUsage: ./launch_test.sh <task> <dataset> [PARAMS]\e[0m"
    exit 1
fi

echo -e "Testing \e[32m$1\e[0m"

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
  if [[ $arg == *RESULT* ]]; then
    resPath=${arg#resultPath=}
    break
  fi
done

docker build \
--build-arg jarpath=${task%_*}/out/artifacts/${task}_jar/ \
--build-arg dataset=${ds} \
--build-arg modeldata=${task%_*}${modelDataArg}/ \
--build-arg modeldir=${modelDataArg} \
--build-arg respath=${resPath} \
--build-arg main-jar=${task}.jar \
--build-arg parameters="$@" \
--no-cache --progress=plain -t ${task}-container-image .

echo -e "\e[38;5;39m${task} built successfully\e[0m"

docker run -it -v $(pwd)/${task}-RESULT:/volume kmeans-container-image