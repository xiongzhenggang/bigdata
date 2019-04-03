package com.xzg.wordsplit;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Chapter3 {

    public static void main(String[] args) {
        try {
            String filename = "hdfs://10.11.91.225:9000/user/input/data.txt";
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://10.11.91.225:9000");
//            conf.set("mapreduce.jobtracker.address", "10.11.91.255:9000");
            // 这个解决hdfs问题
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            // 这个解决本地file问题
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            FileSystem fs = FileSystem.get(conf);
            if(fs.exists(new Path(filename)))
            {
                System.out.println("文件存在");
//                fs.
            }else{
                System.out.println("文件不存在");
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
