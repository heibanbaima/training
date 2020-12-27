package hadoop.hdfs;

/**
 * Created by cxy on 2020/12/27
 **/
public class WCMapperIM implements WCMapper {
    @Override
    public void map(String line, WCContext context) {
        String[] words = line.split("\t");
        for (String word : words) {
            Object value = context.get(word);
            if (value == null) {
                context.write(word, 1);
            } else {
                int v = Integer.parseInt(value.toString());
                context.write(word, v + 1);
            }
        }
    }
}
