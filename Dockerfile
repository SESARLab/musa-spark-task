FROM eclipse-temurin:8-jre

# Path to all the jar files
ARG jarpath
# Path to the dataset
ARG dataset
# Path to the model data (if needed)
ARG modeldata
# Path to the model directory (if needed)
ARG modeldir
# Main jar file
ARG main-jar
# Parameters for the main class
ARG parameters
# Path to the result directory
ARG respath

ADD $jarpath /jars
ADD $dataset /
ADD $modeldata $modeldir

# Create a volume for the output directory
VOLUME /volume

# Run the task and copy the output directory to the volume when the container stops
CMD /usr/lib/jvm/java-1.8.0-amazon-corretto/bin/java -jar /jars/$main-jar $parameters && cp -r $respath /volume