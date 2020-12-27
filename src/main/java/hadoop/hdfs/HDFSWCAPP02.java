package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.net.URI;
import java.util.Properties;

/**
 * Created by cxy on 2020/12/27
 **/
public class HDFSWCAPP02 {

    public static void main(String[] args) throws Exception {

        //1）读取HDFS上的文件 ==> HDFS API
        Properties properties = ParamsUtils.getProperties();
        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));

        //获取要操作的HDFS文件系统
        FileSystem fs = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)),new Configuration(),"hadoop");
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input,false);

        //
    }
}
