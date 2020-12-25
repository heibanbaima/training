package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import sun.nio.ch.IOUtil;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by cxy on 2020/12/25
 **/
public class HDFSApp2 {
    public static final String HDFS_PATH = "hdfs://hadoop000:8020";
    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("------setUp------");
        configuration = new Configuration();
        configuration.set("dfs.replication","1");
        //构造一个访问指定HDFS系统的客户端对象
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");
    }
    /*
     * 创建HDFS文件夹
     */
    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /*
     * 查看HDFS内容
     */
    @Test
    public void text() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/cdh_version.properties"));
        IOUtils.copyBytes(in,System.out,1024);
    }

    /*
     * 创建文件
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        out.writeUTF("hello cxy");
        out.flush();
        out.close();
    }
    @Test
    public void createtwo() throws IOException {
        FSDataOutputStream outtwo = fileSystem.create(new Path("/hdfsapi/test/b.txt"));
        outtwo.writeUTF("hello two cxy");
        outtwo.flush();
        outtwo.close();
    }

    /*
     * 重命名
     */
    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfsapi/test/b.txt");
        Path newPath = new Path("/hdfsapi/test/c.txt");
        boolean result = fileSystem.rename(oldPath,newPath);
        System.out.println(result);
    }

    /*
     * 拷贝本地文件到HDFS文件系统
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        Path src = new Path("C:/Users/ccxy/Desktop/12/组会/服务器.txt");
        Path dst = new Path("/hdfsapi/test/");
        fileSystem.copyFromLocalFile(src,dst);
    }

    /*
     * 拷贝大文件到HDFS文件系统，带进度
     */
    @Test
    public void copyFromLocalBigFile() throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("D:/setup/W.P.S.982801.12012.2019.exe")));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfsapi/test/wps.exe"),
                new Progressable(){
                    public void progress(){
                        System.out.print(".");
                    }
                });
        IOUtils.copyBytes(in,out,4096);
    }

    /*
     * 拷贝HDFS文件到本地：下载
     */
    @Test
    public void copyToLocalFile() throws IOException {
        Path src = new Path("/hdfsapi/test/c.txt");
        Path dst = new Path("E:/hadoop");
        fileSystem.copyToLocalFile(false,src,dst,true);
    }

    @After
    public void tearDown(){
        configuration = null;
        fileSystem = null;
        System.out.println("------tearDown------");
    }
}