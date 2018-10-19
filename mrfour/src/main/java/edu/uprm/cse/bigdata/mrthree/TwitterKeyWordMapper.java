package edu.uprm.cse.bigdata.mrfour;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

/**
 * Created by julian_cuevas1 on 10/17/18.
 */
public class TwitterKeyWordMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();
	
        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
	   
	    if(status.getInReplyToStatusId() > 0){
                 context.write(new Text(Long.toString(status.getInReplyToStatusId())), new Text(Long.toString(status.getId())));   
            }
	}
        catch(TwitterException e){

        }

    }
}
