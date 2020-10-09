package com.atguigu.myoutputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class MyRecoreWriter extends RecordWriter<LongWritable, Text> {

    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;

    /**
     * 初始化方法
     * @param job
     */
    public void initialize(TaskAttemptContext job) throws IOException {
       // atguigu = new FileOutputStream("d:/atguigu.log");
       // other = new FileOutputStream("d:/other.log");
        Configuration configuration = job.getConfiguration();
        //获取文件系统
        FileSystem fileSystem = FileSystem.get(configuration);



    }
    /**
     * KV 写出
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        //先转换一行数据，并加换行符
        String line = value.toString() + '\n';
        // 判断是否包含 atguigu
        if (line.contains("atguigu")){
            atguigu.write(line.getBytes());
        }else {
            other.write(line.getBytes());
        }


    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (atguigu!= null){
            atguigu.close();
        }

        if (other!=null){
            other.close();
        }

    }
}
