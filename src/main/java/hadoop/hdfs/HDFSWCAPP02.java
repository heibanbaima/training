package hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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

        //通过反射创建对象
        Class<?> clazz = Class.forName(properties.getProperty(Constants.WC_MAPPER));
        WCMapper mapper = (WCMapper) clazz.newInstance();

        WCContext context = new WCContext();

        while (iterator.hasNext()){
            LocatedFileStatus file = iterator.next();
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while ((line = reader.readLine()) != null){
                mapper.map(line,context);
            }

            reader.close();
            in.close();
        }

        Map<Object,Object> contextMap = context.getCacheMap();

        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));

        FSDataOutputStream out = fs.create(new Path(output,new Path(properties.getProperty(Constants.OUTPUT_FILE))));

        Set<Map.Entry<Object,Object>> entries = contextMap.entrySet();
        for (Map.Entry<Object,Object> entry:entries){
            out.write((entry.getKey().toString()+"\t"+entry.getValue()+"\n").getBytes());
        }

        out.close();
        fs.close();

        System.out.println("cxy的词频统计运行成功。。。");

    }
}
