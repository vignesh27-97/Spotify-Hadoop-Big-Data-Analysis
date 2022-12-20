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
public class TrendingMusicTitles {

    public static class Spotify_Mapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] tokens = value.toString().split(",");
            
            Text year = new Text();
            
            String[] date = tokens[2].split("-");
            year.set(date[0]);

            Text title = new Text(tokens[0]);
            
            context.write(year, title);
            
        }
    }

    public static class Spotify_Reducer extends Reducer<Text, Text, Text, Text> {

        private Text topMusicTitle = new Text(); 

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            HashMap<String, Integer> titleCounts = new HashMap();
            
            int totalTitles = 0;
                
            for(Text val: values) {
                  
                String title = val.toString();
                int count = 0;
                
                if(titleCounts.containsKey(title)){
                    count = titleCounts.get(title);
                } 
                
                titleCounts.put(title, count + 1);
                totalTitles += 1;
                
            }
            
            String topTitle = getTopArtistByRegion(titleCounts);
            
            topMusicTitle.set(topTitle);
            
            context.write(key, topMusicTitle);
            
        }
        
        private String getTopArtistByRegion(Map<String, Integer> titleCounts) {
            
            String topTitle = "";
            
            int maxCount = 0;
            
            for (Map.Entry<String, Integer> titleCount : titleCounts.entrySet()) {
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
        Job job = Job.getInstance(conf, "Trending music titles every year");
        job.setJarByClass(TrendingMusicTitles.class);
        
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
