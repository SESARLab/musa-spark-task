# Timeline di lavoro
- Estratti i task dalla cartella completa, ogni singolo task inserito in un progetto a se stante.
- Per ogni task, creato un file `pom.xml` con le dipendenze, oltre a quelle che servono a Spark, la maggior parte dei task necessita solamente di `VectorDisassembler`, con rare eccezioni che necessitano di altre librerie.
- Aggiornamento della versione di Spark alla 3.5.0 (Scala 2.12):
	- Per consentire l'utilizzo di VectorDisassembler: fork della repository, ricostruendo il jar con Scala 2.12.
- Riorganizzato il file delle dipendenze in modo tale che i task che sono variazioni dello stesso algoritmo siano nello stesso progetto (con un parent comune), in più ho configurato il plugin `maven-assembly-plugin` per il packaging dell'Uber jar.
- Esecuzione dei task in container Docker, caricando l'Uber jar e il dataset direttamente nell'immagine Docker.
- Creazione di un programma di digit recognition con TensorFlow, con relativa esecuzione all'interno di container Docker.
- Esecuzione dei task in cluster Kubernetes locale:
	- Cluster avviato da docker-desktop
	- Pull dell'immagine Docker costruita precedentemente su docker hub
	- Submission del job Spark utilizzando il tool `spark-submit`, indicando come master l'indirizzo del Control Plane del cluster, e come immagine quella caricata su docker hub (il dataset è incluso nell'immagine). La configurazione delle Spark properties legate all'esecuzione in cluster non avviene con le API Java di Spark, ma con i parametri di `spark-submit`.
- Esecuzione dei task in cluster Kubernetes su Amazon EMR on EKS.
- Test di `spark-submit` per verificare l'effettiva distribuzione del lavoro su più nodi del cluster.
	- L'analisi dei logs e dello stato del cluster in un deployment del task kmeans su cluster Kubernetes locale evidenzia il comportamento atteso.
- Training distribuito su cluster kubernetes del modello TensorFlow.

---

## Create the `VectorAssembler` dependency jar

Git clone [this repo](https://github.com/jamesbconner/VectorDisassembler/tree/master) and replace the POM with the following one:  
  
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.apache.spark.ml.feature</groupId>
    <artifactId>VectorDisassembler</artifactId>
    <version>0.1</version>

    <properties>
        <spark_version>3.5.3</spark_version>
        <scala_version>2.12</scala_version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${scala_version}</artifactId>
            <version>${spark_version}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala_version}</artifactId>
            <version>${spark_version}</version>
            <scope>provided</scope>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-mllib_${scala_version}</artifactId>
            <version>${spark_version}</version>
            <scope>provided</scope>
        </dependency>
    </dependencies>

    <build>
        <sourceDirectory>src/main/scala</sourceDirectory>
        <plugins>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>4.9.2</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>

</project>
```

Then run `mvn package` to create the `VectorDisassembler-0.1.jar` file. You can find this jar in `./code` folder. Finally, install the dependency into the local maven repository:

```bash
mvn install:install-file -Dfile=/path/to/VectorDisassembler-0.1.jar \
                         -DgroupId=org.apache.spark.ml.feature \
                         -DartifactId=VectorDisassembler \
                         -Dversion=1.0 \
                         -Dpackaging=jar
```

## Author
Gabriele Stentella
