package com.xzg.wordsplit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

// //这个Mapper类是一个泛型类型，它有四个形参类型，分别指定map函数的输入键、输入值、输出键、输出值的类型
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    //该方法循环调用，从文件的split中读取每行调用一次，把该行所在的下标为key，该行的内容为value
    protected void map(LongWritable key, Text value,
                       Context context)
            throws IOException, InterruptedException {
        String[] words = StringUtils.split(value.toString(), ' ');
        for(String w :words){
            //a,1 a,2 重新组装a:1 a:2
            String[] kevs = w.split(",");
            context.write(new Text(kevs[0]), new IntWritable(Integer.valueOf(kevs[1])));
        }
    }
}