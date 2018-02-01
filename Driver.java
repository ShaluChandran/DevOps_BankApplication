package com.target.dl;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.orc.OrcConf;

/**
 * Created by z076053 on 1/28/18.
 */
public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final FileReader reader = new FileReader(
                "C:/Users/SHALCHA/Favorites/Downloads/Shalu/DI-WSL-master/DI-WSL-master/src/main/resources/application.properties");

        final Properties p = new Properties();
        p.load(reader);

        final Configuration configuration = new Configuration();
        final String schema = p.getProperty("hive_schema");
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(configuration, schema);

        new GenericOptionsParser(configuration, args).getRemainingArgs();
        // final Job job = Job.getInstance(configuration, "ORC Driver");
        // job.setJarByClass(Driver.class);
        // // job.setMapperClass(ORCMapper.class);
        // job.setMapperClass(ORCJsonMapper.class);
        // job.setOutputFormatClass(OrcOutputFormat.class);
        // job.setOutputKeyClass(NullWritable.class);
        // job.setOutputValueClass(OrcStruct.class);
        // job.setNumReduceTasks(0);
        System.out.println(p.getProperty("input_path"));
        System.out.println(p.getProperty("output_path"));
        System.out.println(p.getProperty("hive_schema"));
        final String input = p.getProperty("input_path");
        System.out.println(input);
        final FileSystem hdfs = FileSystem.get(configuration);
        final Path in = new Path(p.getProperty("input_path"));
        final FileStatus[] fileStatus = hdfs.listStatus(in);
        // final FileStatus[] fileStatus = hdfs.listStatus(new Path("C:/Shalu_56532/MyDocs/Parking/BackUp/parkingaround"));
        for (final FileStatus status : fileStatus) {
            System.out.println(status.getPath().toString());
        }

    }
}
