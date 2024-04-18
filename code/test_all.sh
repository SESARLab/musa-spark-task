#! /bin/zsh

./launch_test.sh kmeans dataset.csv /dataset.csv 5 /RESULT &&
./launch_test.sh kmeans_model ds_nonum.csv csvData=/ds_nonum.csv k=5 resultPath=/RESULT-model &&
./launch_test.sh kmeans_predict ds_nonum.csv csvData=/ds_nonum.csv k=5 modelData=/RESULT-model resultPath=/RESULT-predict &&
./launch_test.sh bkmeans dataset.csv /dataset.csv 5 /RESULT &&
./launch_test.sh bkmeans_model ds_nonum.csv csvData=/ds_nonum.csv k=5 resultPath=/RESULT-model &&
./launch_test.sh bkmeans_predict ds_nonum.csv csvData=/ds_nonum.csv k=5 modelData=/RESULT-model resultPath=/RESULT-predict &&
echo -e "\e[32mSUCCESS\e[0m"