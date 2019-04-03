package com.xzg.wordsplit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {

    public static void main(String[] args) {
        Configuration config =new Configuration();
//        config.set("fs.defaultFS", "hdfs://HadoopMaster:9000");
        config.set("fs.defaultFS", "hdfs://10.11.91.225:9000");
        //node22为hadoopyarn-site.xml中的配置
        config.set("yarn.resourcemanager.hostname", "node22");
        //设置执行的用户，需要是服务端的hadoop用户，否则无权限执行，报错.AccessControlException: Permission denied
        System.setProperty("HADOOP_USER_NAME", "admin");
//    config.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");//先打包好wc.jar
        try {
            FileSystem fs =FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJarByClass(RunJob.class);
            job.setJobName("wc");
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path("/user/input/wc.txt"));//新建好输入路径，且数据源
            Path outpath =new Path("/user/output/wc");
            if(fs.exists(outpath)){
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);
            boolean f= job.waitForCompletion(true);
            if(f){
                System.out.println("job任务执行成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
