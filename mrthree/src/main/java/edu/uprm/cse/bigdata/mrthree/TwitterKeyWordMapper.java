package edu.uprm.cse.bigdata.mrthree;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

import java.io.IOException;

/**
 * Created by julian_cuevas1 on 10/17/18.
 */
public class TwitterKeyWordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);

            context.write(new Text(status.getUser().getScreenName()), new IntWritable(1));
	  
        }
        catch(TwitterException e){

        }

    }
}
