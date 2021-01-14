package hadoop.hdfs;

/**
 * Created by cxy on 2021/1/6
 **/
public class WCMapperCIIM implements WCMapper{

    @Override
    public void map(String line, WCContext context) {
        String[] words = line.toLowerCase().split("\t");

        for (String word : words){
            Object value = context.get(word);
            if (value == null){
                context.write(word,1);
            }else {
                int v = Integer.parseInt(value.toString());
                context.write(word,v+1);
            }
        }
    }
}
