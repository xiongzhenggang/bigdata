package com.xzg.wordsplit;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {
    //为什么这里k1要用Object、Text、IntWritable等，而不是java的string啊、int啊类型，当然，你可以用其他的，这样用的好处是，因为它里面实现了序列化和反序列化。
    public static class TokenizerMapper
            //这个Mapper类是一个泛型类型，它有四个形参类型，分别指定map函数的输入键、输入值、输出键、输出值的类型。hadoop没有直接使用Java内嵌的类型，而是自己开发了一套可以优化网络序列化传输的基本类型。这些类型都在org.apache.hadoop.io包中。
            //比如这个例子中的Object类型，适用于字段需要使用多种类型的时候，Text类型相当于Java中的String类型，IntWritable类型相当于Java中的Integer类型
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.11.91.225:9000");
        conf.set("mapreduce.jobtracker.address", "10.11.91.225:9000");
        // 这个解决hdfs问题
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        // 这个解决本地file问题
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
