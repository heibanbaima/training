package hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by cxy on 2021/1/7
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * (hello,1)  (world,1)
     * (hello,1)  (world,1)
     * (hello,1)  (world,1)
     * (welcome,1)
     * <p>
     * reduce1： (hello,1)(hello,1)(hello,1)  ==> (hello, <1,1,1>)
     * reduce2: (world,1)(world,1)(world,1)   ==> (world, <1,1,1>)
     * reduce3: (welcome,1)  ==> (welcome, <1>)
     */

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
            IntWritable value = iterator.next();
            count += value.get();
        }

        context.write(key, new IntWritable(count));
    }
}
