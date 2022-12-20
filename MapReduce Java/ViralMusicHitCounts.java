/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Other/File.java to edit this template
 */

package com.mycompany.finalproject_spotifycharts;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class ViralMusicHitCounts {

    public static class Spotify_Mapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] tokens = value.toString().split(",");
            Text region = new Text();
            region.set(tokens[5]);

            // passing the region as key and category as values
            context.write(region, new Text(tokens[6]));
        }
    }

    public static class Spotify_Reducer extends Reducer<Text, Text, Text, IntWritable> {

        private IntWritable sumObj = new IntWritable(); 

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            
            for(Text val: values) {
                     
                // checking if the category is viral50 and then adds the sum varibale by 1
                if(val.toString().equals("viral50")){
                    sum += 1;
                } 
                
            }
            
            sumObj.set(sum);
            
            context.write(key, sumObj);
            
        }
        
    }


    public static void main (String[] args) throws Exception  {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Viral Hits category counts based on Region");
        job.setJarByClass(ViralMusicHitCounts.class);
        
        job.setMapperClass(Spotify_Mapper.class);
        job.setReducerClass(Spotify_Reducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
    
}
