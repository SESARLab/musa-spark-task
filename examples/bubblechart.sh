/opt/spark/bin/spark-submit \
    --class it.unimi.evotion.tasks.bubblechart.bcTask \
    jarpath \
    csvData=sample_data.csv \
    col1=x \
    col2=y \
    mag=size \
    title="Example bubblechart" \
    sname=DemoSeries \
    fileName=bubblechart.png \
    folderName=/user/bda/graph \
    width=800 \
    height=600
