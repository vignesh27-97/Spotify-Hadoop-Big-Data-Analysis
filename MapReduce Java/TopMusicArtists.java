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
public class TopMusicArtists {

    public static class Spotify_Mapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] tokens = value.toString().split(",");
            Text region = new Text();
            region.set(tokens[5]);

            // passing region as key and artist as value
            context.write(region, new Text(tokens[3]));
        }
    }

    public static class Spotify_Reducer extends Reducer<Text, Text, Text, Text> {

        private Text topMusicArtist = new Text(); 

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            // Storing the artist and their count as a key value pairs in HashMap
            HashMap<String, Integer> artistCounts = new HashMap();
            
            int totalArtists = 0;
                
            for(Text val: values) {
                  
                String artist = val.toString();
                int count = 0;
                
                // checking for the same artist
                if(artistCounts.containsKey(artist)){
                    // get the count of the respective artist stored in the Hash Map variable
                    count = artistCounts.get(artist);
                } 
                
                artistCounts.put(artist, count + 1);
                totalArtists += 1;
                
            }
            
            // Calling the function to get the top counted artist
            String topArtist = getTopArtistByRegion(artistCounts);

            topMusicArtist.set(topArtist);
            
            context.write(key, topMusicArtist);
            
        }
        
        // Getting the Top Artists for every region
        private String getTopArtistByRegion(Map<String, Integer> artistCounts) {
            
            String topArtist = "";
            
            int maxCount = 0;
            
            // entrySet returns a set view of the hash map
            for (Map.Entry<String, Integer> artistCount : artistCounts.entrySet()) {
                
                // checking if count value > maxCount value
                if (artistCount.getValue() > maxCount) {
                    maxCount = artistCount.getValue();
                    topArtist = artistCount.getKey();
                }
                
            }
            
            return topArtist;
            
        }
        
    }

    // Adding Combiner function to summarize the map output records with the same key
    public static class Spotify_Combiner extends Reducer<Text, Text, Text, Text> {

        private Text topMusicArtist = new Text(); 

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            // Storing the artist and their count as a key value pairs in HashMap
            HashMap<String, Integer> artistCounts = new HashMap();
            
            int totalArtists = 0;
                
            for(Text val: values) {
                  
                String artist = val.toString();
                int count = 0;
                
                // checking for the same artist
                if(artistCounts.containsKey(artist)){
                    // get the count of the respective artist stored in the Hash Map variable
                    count = artistCounts.get(artist);
                } 
                
                artistCounts.put(artist, count + 1);
                totalArtists += 1;
                
            }
            
            // Calling the function to get the top counted artist
            String topArtist = getTopArtistByRegion(artistCounts);

            topMusicArtist.set(topArtist);
            
            context.write(key, topMusicArtist);
            
        }
        
        // Getting the Top Artists for every region
        private String getTopArtistByRegion(Map<String, Integer> artistCounts) {
            
            String topArtist = "";
            
            int maxCount = 0;
            
            // entrySet returns a set view of the hash map
            for (Map.Entry<String, Integer> artistCount : artistCounts.entrySet()) {
                
                // checking if count value > maxCount value
                if (artistCount.getValue() > maxCount) {
                    maxCount = artistCount.getValue();
                    topArtist = artistCount.getKey();
                }
                
            }
            
            return topArtist;
            
        }
        
    }


    public static void main (String[] args) throws Exception  {
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Music Artists based on Region");
        job.setJarByClass(TopMusicArtists.class);
        
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
