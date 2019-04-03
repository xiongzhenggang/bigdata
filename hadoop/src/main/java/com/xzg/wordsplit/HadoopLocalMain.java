package com.xzg.wordsplit;

public class HadoopLocalMain {
    //System.setProperty("HADOOP_USER_NAME", "root");
//
//1、MR执行环境有两种：本地测试环境，服务器环境
//
//本地测试环境(windows)：(便于调试)
// 在windows的hadoop目录bin目录有一个winutils.exe
// 1、在windows下配置hadoop的环境变量
// 2、拷贝debug工具(winutils.exe)到HADOOP_HOME/bin
// 3、修改hadoop的源码 ，注意：确保项目的lib需要真实安装的jdk的lib
//
// 4、MR调用的代码需要改变：
// a、src不能有服务器的hadoop配置文件(因为，本地是调试，去服务器环境集群那边的)
// b、再调用是使用：
// Configuration config = new Configuration();
// config.set("fs.defaultFS", "hdfs://HadoopMaster:9000");
// config.set("yarn.resourcemanager.hostname", "HadoopMaster");

//服务器环境：(不便于调试)，有两种方式。
//首先需要在src下放置服务器上的hadoop配置文件（都要这一步）
//1、在本地直接调用，执行过程在服务器上（真正企业运行环境）
// a、把MR程序打包（jar），直接放到本地
// b、修改hadoop的源码 ，注意：确保项目的lib需要真实安装的jdk的lib
// c、增加一个属性：
// config.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
// d、本地执行main方法，servlet调用MR。
//
//
//2、直接在服务器上，使用命令的方式调用，执行过程也在服务器上
// a、把MR程序打包（jar），传送到服务器上
// b、通过： hadoop jar jar路径 类的全限定名

}
