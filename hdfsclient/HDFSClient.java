package com.fangwin.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

public class HDFSClient {
    private FileSystem fs;
    private String IP = "192.168.52.132";

    @Before
    public void before() throws IOException, InterruptedException {
        System.out.println("Before");
        fs = FileSystem.get(URI.create("hdfs://"+this.IP+":9000"),
                new Configuration(), "hadoop");
    }

    @After
    public void after() throws IOException {
        System.out.println("After");
        fs.close();
    }

    @Test
    public void putTest() throws IOException, InterruptedException {
        //获取HDFS抽象对象
        FileSystem fileSystem = FileSystem.get(
                URI.create("hdfs://"+this.IP+":9000"),
                new Configuration(),
                "hadoop");
        //用这个对象操作文件系统
        fileSystem.copyFromLocalFile(
                new Path("D:\\work\\hadoop\\test\\word.txt"), new Path("/test"));
        //关闭文件系统
        fileSystem.close();
    }

    @Test
    public void putByOtherConfigration() throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.setInt("dfs.replication", 1);
        fs = FileSystem.get(
                URI.create("hdfs://"+this.IP+":9000"), configuration, "hadoop");
        fs.copyFromLocalFile(
                new Path("D:\\work\\hadoop\\test\\1.txt"),
                new Path("/test/2.txt"));
    }

    @Test
    public void getTest() throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(
                URI.create("hdfs://"+this.IP+":9000"),
                new Configuration(),
                "hadoop");
        fileSystem.copyToLocalFile(
                new Path("/1.txt"), new Path("D:\\work\\hadoop\\test"));
        fileSystem.close();
    }

    @Test
    public void renameTest() throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(
                URI.create("hdfs://"+this.IP+":9000"),
                new Configuration(),
                "hadoop");
        fileSystem.rename(
                new Path("/3.txt"), new Path("/5.txt"));
        fileSystem.close();
    }

    @Test
    public void deleteTest() throws IOException {
        boolean delete = fs.delete(new Path("/5.txt"), true);
        if (delete) {
            System.out.println("删除成功");
        } else {
            System.out.println("删除失败");
        }
    }

    @Test
    public void mkdirTest() throws IOException {
        boolean isok = fs.mkdirs(new Path("/test"));
        if (isok) {
            System.out.println("mkdir success");
        } else {
            System.out.println("mkdir fall");
        }
    }

    @Test
    public void appendTest() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/1.txt"), 1024);
        /*1.本地文件追加*/
//        FileInputStream open = new FileInputStream("D:\\work\\hadoop\\test\\testAppend.txt");
        /*2.HDFS文件追加*/
        FSDataInputStream open = fs.open(new Path("/3.txt"));
        //拷贝流(自带close2个流)
        IOUtils.copyBytes(open, append, 1024, true);
    }

    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("以下信息是一个文件的信息：");
                System.out.println("Path:" + fileStatus.getPath());
                System.out.println("Length:" + fileStatus.getLen());
                System.out.println(fileStatus.getModificationTime());
            } else {
                System.out.println("以下信息是一个文件或者软连接");
                System.out.println("Path:" + fileStatus.getPath());
            }
        }
    }

    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> files =
                fs.listFiles(new Path("/"), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            System.out.println("Path:" + file.getPath());

            System.out.println("Block:");
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("Block_Host:" + host);
                }
            }
        }
    }
}
