package com.atguigu.myoutputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutpurFormat extends FileOutputFormat<LongWritable, Text> {
    /**
     * 返回recordwriter 把数据写成两个文件
     * @param job
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        MyRecoreWriter writer = new MyRecoreWriter();
        writer.initialize(job);
        return writer;
    }
}
