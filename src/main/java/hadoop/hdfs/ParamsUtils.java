package hadoop.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by cxy on 2020/12/27
 **/
public class ParamsUtils {

    private static Properties properties = new Properties();
    static {
        try {
            properties.load(ParamsUtils.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() throws Exception{
        return properties;
    }
}
