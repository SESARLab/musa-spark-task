package it.unimi.evotion.tasks.bubblechart;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.LogManager;

public class HdfsWriter extends Configured {

    private Map<String, String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private Logger logger;

    public HdfsWriter(Map<String, String> parameters){
        this.parameters = parameters;
    }

    public void createFile(String newFolder, String filename, String localInputPath) throws IOException
    {
        Path newFolderPath= new Path(newFolder);
        FileSystem hdfs = null;

        this.logger = LogManager.getLogger(bcTask.class);

        try {
            //hdfs = FileSystem.get(new URI("hdfs://172.20.28.13:8020"), new Configuration(), "bda");
            hdfs = FileSystem.get(new URI(parameters.get("URI")), new Configuration(), parameters.get("user"));

            if(!hdfs.exists(newFolderPath))
            {
                hdfs.mkdirs(newFolderPath);
            }

            Path newFilePath=new Path(newFolder+"/"+filename);

            if(hdfs.exists(newFilePath))
            {
                hdfs.delete(newFilePath, true);
            }

            /* OLD METHOD
            byte[] byt=sb.toString().getBytes();
            FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
            fsOutStream.write(byt);
            fsOutStream.close();
            ret = ""+newFilePath;
            */

            FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
            InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));
            IOUtils.copyBytes(is, fsOutStream, 4096, true);
            hdfs.close();

        } catch (URISyntaxException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void createFileForTablesaw(String newFolder, String filename, String page) throws IOException
    {
        Path newFolderPath= new Path(newFolder);

        FileSystem hdfs = null;

        try {
            //hdfs = FileSystem.get(new URI("hdfs://172.20.28.13:8020"), new Configuration(), "bda");

            hdfs = FileSystem.get(new URI(parameters.get("URI")), new Configuration(), parameters.get("user"));

            if(!hdfs.exists(newFolderPath))
            {
                hdfs.mkdirs(newFolderPath);
            }


            Path newFilePath=new Path(newFolder+"/"+filename);

            if(hdfs.exists(newFilePath))
            {
                hdfs.delete(newFilePath, true);
            }

            /* OLD METHOD
            byte[] byt=sb.toString().getBytes();
            FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
            fsOutStream.write(byt);
            fsOutStream.close();
            */

            FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
            InputStream is = new ByteArrayInputStream(page.getBytes(Charset.forName("UTF-8")));
            IOUtils.copyBytes(is, fsOutStream, 4096, true);
            hdfs.close();

        } catch (URISyntaxException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}