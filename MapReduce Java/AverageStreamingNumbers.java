/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Other/File.java to edit this template
 */

package com.mycompany.finalproject_spotifycharts;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author vignesh
 */

public class AverageStreamingNumbers {


    public static class Spotify_Mapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] tokens = value.toString().split(",", -1);
            Text region = new Text();
            region.set(tokens[5]);

            // passing the region as key and streams as a value
            context.write(region, new Text(new Text(tokens[8])));
            
        }
        
    }

    public static class Spotify_Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private DoubleWritable avgObj = new DoubleWritable(); 

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            double sum = 0;
            int count = 0;
            
            for(Text val: values) {  
                
                // is stream value is empty then adding the value as 0
                if(val.toString().equals("")){
                    sum += 0;
                }
                else{
                    double temp = Double.parseDouble(val.toString());
                    sum += temp;    
                }
                
                count += 1;
 
            }
            
            double avg = sum/count;
            avgObj.set(avg);
            
            context.write(key, avgObj);
            
        }
        
    }
   
    // An average is not an associative operation
    // but if the count is output from the reducer with the sum of salary
    // these two values can be multiplied to preserve the sum for the final reduce phase
    
    
    public static void main (String[] args) throws Exception  {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Streaming Numbers based on Regions");
        job.setJarByClass(AverageStreamingNumbers.class);
        
        job.setMapperClass(Spotify_Mapper.class);
        job.setReducerClass(Spotify_Reducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
    
}

