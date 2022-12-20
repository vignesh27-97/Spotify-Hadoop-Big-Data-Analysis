/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
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

public class TopMusicTitles {

    public static class Spotify_Mapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] tokens = value.toString().split(",");
            
            Text region = new Text();
            region.set(tokens[5]);  

            Text title = new Text(tokens[0]);  
            
            // passing region as key and title as value
            context.write(region, title);
            
        }
        
    }

    public static class Spotify_Reducer extends Reducer<Text, Text, Text, Text> {

        private Text topMusicTitle = new Text(); 

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            // Storing the title and it's count as a key value pairs in HashMap
            HashMap<String, Integer> titleCounts = new HashMap();
            
            int totalTitles = 0;
                
            for(Text val: values) {
                  
                String title = val.toString();
                int count = 0;
                
                // checking for the same title
                if(titleCounts.containsKey(title)){
                    // get the count of the respective title stored in the Hash Map variable
                    count = titleCounts.get(title);
                } 
                
                titleCounts.put(title, count + 1);
                totalTitles += 1;
                
            }
            
            // Calling the function to get the top counted title
            String topTitle = getTopTitlesByRegion(titleCounts);
            
            topMusicTitle.set(topTitle);
            
            context.write(key, topMusicTitle);
            
        }
        
        // Getting the Top Titles for every region
        private String getTopTitlesByRegion(Map<String, Integer> titleCounts) {
            
            String topTitle = "";
            
            int maxCount = 0;
            
            // entrySet returns a set view of the hash map
            for (Map.Entry<String, Integer> titleCount : titleCounts.entrySet()) {
                
                // checking if count value > maxCount value
                if (titleCount.getValue() > maxCount) {
                    maxCount = titleCount.getValue();
                    topTitle = titleCount.getKey();
                }
                
            }
            
            return topTitle;
            
        }
        
    }

    // Adding Combiner function to summarize the map output records with the same key
    public static class Spotify_Combiner extends Reducer<Text, Text, Text, Text> {

        private Text topMusicTitle = new Text(); 

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            // Storing the title and it's count as a key value pairs in HashMap
            HashMap<String, Integer> titleCounts = new HashMap();
            
            int totalTitles = 0;
                
            for(Text val: values) {
                  
                String title = val.toString();
                int count = 0;
                
                // checking for the same title
                if(titleCounts.containsKey(title)){
                    // get the count of the respective title stored in the Hash Map variable
                    count = titleCounts.get(title);
                } 
                
                titleCounts.put(title, count + 1);
                totalTitles += 1;
                
            }
            
            // Calling the function to get the top counted title
            String topTitle = getTopTitlesByRegion(titleCounts);
            
            topMusicTitle.set(topTitle);
            
            context.write(key, topMusicTitle);
            
        }
        
        // Getting the Top Titles for every region
        private String getTopTitlesByRegion(Map<String, Integer> titleCounts) {
            
            String topTitle = "";
            
            int maxCount = 0;
            
            // entrySet returns a set view of the hash map
            for (Map.Entry<String, Integer> titleCount : titleCounts.entrySet()) {
                
                // checking if count value > maxCount value
                if (titleCount.getValue() > maxCount) {
                    maxCount = titleCount.getValue();
                    topTitle = titleCount.getKey();
                }
                
            }
            
            return topTitle;
            
        }
        
    }

    
    public static void main (String[] args) throws Exception  {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Music Charts based on Region");
        job.setJarByClass(TopMusicTitles.class);
        
        job.setMapperClass(Spotify_Mapper.class);
        job.setReducerClass(Spotify_Reducer.class);
        job.setCombinerClass(Spotify_Combiner.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
        
    }
    
}
