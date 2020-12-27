package hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义缓存
 **/
public class WCContext {

    private Map<Object,Object> cacheMap = new HashMap<Object,Object>();

    public Map<Object,Object> getCacheMap(){
        return cacheMap;
    }

    //写数据到缓存中
    public void write(Object key,Object value){
        cacheMap.put(key,value);
    }

    //从缓存中获取值
    public Object get(Object key){
        return cacheMap.get(key);
    }
}
