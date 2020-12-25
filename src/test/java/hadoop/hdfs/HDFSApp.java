package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * 使用Java API操作HDFS文件系统
 * 1）创建Configuration
 * 2）获取FileSystem
 * 3）HDFS API的操作
 **/
public class HDFSApp {
    public static void main(String[] args) throws Exception {

        // hadoop000:8020
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"),configuration,"hadoop");
        Path path = new Path("/hdfsapi/test");
        boolean result = fileSystem.mkdirs(path);

        System.out.println(result);
    }
}
