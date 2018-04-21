package com.shalu.dl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        Job job = Job.getInstance(configuration,"ORC Driver");
        job.setJarByClass(Driver.class);
        job.setMapperClass(ORCJsonMapper.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0);

        //Add named output for Error Records
        MultipleOutputs.addNamedOutput(job, "ERROR", TextOutputFormat.class, NullWritable.class, Text.class);

        //Add named output for Sucess Records
        MultipleOutputs.addNamedOutput(job, "SUCCESS", OrcNewOutputFormat.class, NullWritable.class, Writable.class);

        MultipleOutputs.setCountersEnabled(job, true);
        MultipleOutputs.getCountersEnabled(job);

        FileSystem hdfs = FileSystem.get(configuration);
        if (hdfs.exists(new Path(otherArgs[1])))
            hdfs.delete(new Path(otherArgs[1]), true);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
