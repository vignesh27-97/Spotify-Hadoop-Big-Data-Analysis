/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Other/File.java to edit this template
 */

package com.mycompany.finalproject_spotifycharts;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class MostStreamedMusicTitles {

    
    public static class Spotify_Mapper extends Mapper<Object, Text, Text, Text> {

        
        @Override
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] tokens = value.toString().split(",", -1);
            
            Text year = new Text();
            
            String[] date = tokens[2].split("-");
            year.set(date[0]);

            // Passing the year as a key and combination of streams and title as value to the reducer
            context.write(year, new Text(tokens[8] + ";" + tokens[0]));
            
        }
    }
    

    public static class Spotify_Reducer extends Reducer<Text, Text, Text, Text> {

        private Text mostStreamedMusicTitle = new Text(); 

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        
            double totalStreams = 0;
            double streams; 
            
            HashMap<String, Double> titleStreams = new HashMap();
                
            for(Text val: values) {
                  
                String[] records = val.toString().split(";",-1);
                
                String title = records[1];
                
                // if there's empty title then assigning the value of the stream to be 0
                if(records[0].equals("")){
                    streams = 0;
                }else{
                    streams = Double.parseDouble(records[0]);
                }
                
                double count = 0;
                
                // checking for the same title
                if(titleStreams.containsKey(title)){
                    // get the count of the respective title stored in the Hash Map variable
                    count = titleStreams.get(title);
                } 
                 
                titleStreams.put(title, count + streams);
                totalStreams += count;

            }
            
            // Calling the function to get the top counted artist
            String topStreamed = getMostStreamedMusic(titleStreams);
            
            mostStreamedMusicTitle.set(topStreamed);
            
            context.write(key, mostStreamedMusicTitle);
            
        }
        
        // Getting the most streamed music titles for every year
        private String getMostStreamedMusic(Map<String, Double> titleStreams) {
            
            String mostStreamedTitle = "";
            
            double maxCount = 0;
            
            // entrySet returns a set view of the hash map
            for (Map.Entry<String, Double> titleStream : titleStreams.entrySet()) {
                
                // checking if count value > maxCount value
                if (titleStream.getValue() > maxCount) {
                    maxCount = titleStream.getValue();
                    mostStreamedTitle = titleStream.getKey();
                }
                
            }
            
            return mostStreamedTitle;
            
        }
        
    }


    
    
    public static void main (String[] args) throws Exception  {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most streamed music titles every year");
        job.setJarByClass(MostStreamedMusicTitles.class);
        
        job.setMapperClass(Spotify_Mapper.class);
        job.setReducerClass(Spotify_Reducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
    

}
