#! /bin/zsh

if [ $# -lt 2 ]
  then
    echo -e "\e[1;33mUsage: ./launchDockerKmeans.sh <task> <dataset> [PARAMS]\e[0m"
    exit 1
fi

echo -e "Testing \e[32m$1...\e[0m"

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
