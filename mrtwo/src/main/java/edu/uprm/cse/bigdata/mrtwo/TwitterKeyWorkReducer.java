package edu.uprm.cse.bigdata.mrtwo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by julian_cuevas1 on 10/17/18.
 */
public class TwitterKeyWorkReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;

        for (IntWritable value : values){
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
