package com.target.mapreduce.main;

import java.io.IOException;

import org.apache.crunch.types.orc.OrcUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TestMain {	
	///Users/z002zls/Java_Workspace/MapReduceMain/src/main/java/com/target/mapreduce/main
	public static void main(String[] args) throws IOException {
	TestMain test = new TestMain();
	String tsvString = "text_string\t1\t2";
	test.createOrcFile(tsvString);
}

public void createOrcFile(String input) throws IOException {
	String typeStr = "struct<name:string,age:int,number:int>";
	TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
	ObjectInspector inspector = OrcStruct.createObjectInspector(typeInfo);

	String[] inputTokens = input.split("\\t");

	OrcStruct orcLine = OrcUtils.createOrcStruct(
			typeInfo,
			new Text(inputTokens[0]),
			new IntWritable(Integer.valueOf(inputTokens[1])),
			new IntWritable(Integer.valueOf(inputTokens[2])));
//			new LongWritable(Long.valueOf(inputTokens[3])),
//			new DoubleWritable(Double.valueOf(inputTokens[4])),
//			new FloatWritable(Float.valueOf(inputTokens[5])));
	Configuration conf = new Configuration();
	Path tempPath = new Path("/user/SVSTRSLS/test_json/test3.orc");

	Writer writer = OrcFile.createWriter(tempPath, OrcFile.writerOptions(conf).inspector(inspector).stripeSize(100000).bufferSize(10000));
	writer.addRow(orcLine);
	writer.close();
}
}
