/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Other/File.java to edit this template
 */

package com.mycompany.finalproject_spotifycharts;

import java.io.IOException;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
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

public class Top20GlobalArtists {
    
    // Job1 - Sum of total streams based on regions
    // Mapper of Job1
    public static class Spotify_Mapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map (Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] tokens = value.toString().split(",", -1);
            Text artist = new Text();
            artist.set(tokens[3]);

            // passing the artist as a key and streams as a value
            context.write(artist, new Text(new Text(tokens[8])));
            
        }
        
    }

    // Reducer of Job1
    public static class Spotify_Reducer extends Reducer<Text, Text, Text, DoubleWritable> {

        private DoubleWritable sumObj = new DoubleWritable(); 

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            double sum = 0;
            
            for(Text val: values) {  
                
                // if there's empty stream value then assigning the value of sum to be 0
                if(val.toString().equals("")){
                    sum += 0;
                }
                else{
                    double temp = Double.parseDouble(val.toString());
                    sum += temp;    
                }

            }
            
            sumObj.set(sum);
            
            context.write(key, sumObj);
            
        }
        
    }

    // Job2 - Top 20 Global Music Artists based on streams
    // Mapper of Job2
    public static class Spotify_Top20Mapper extends Mapper<Object, Text, Text, DoubleWritable> {
        
        private int n;  
        private TreeMap<Double, String> stream_list; 

        // Called once at the beginning of the task
        @Override
        public void setup(Context context) {
            n = 20;
            // local list for sorting stream values
            stream_list = new TreeMap<Double, String>();

        }

        // Called once for each key/value pair in the input split
        @Override
        public void map(Object key, Text value, Context context) {
            
            // Spiltting the input rows by tab space since hadoop stores stores the key value output as same
            String[] line = value.toString().split("\t");   
   
            // adds the total streams as key, and artist as values for sorting based on total stream counts
            // TreeMap sorts based on the keys
            stream_list.put(Double.valueOf(line[1]), line[0]);

            
            // checks if the local stream_list contains more than 20 elements
            if (stream_list.size() > n){
                // removing the first element with the smallest streaming count value
                stream_list.remove(stream_list.firstKey());
            }
                
        }

        // Called once at the end of the task
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            // write the topN streaming artists before proceeding with TopNReducer
            
            for (Map.Entry<Double, String> entry : stream_list.entrySet()) {
                
                String artist = entry.getValue();
                Double stream = entry.getKey();
                
                // passes the artist as a key and total streams as a value
                context.write(new Text(artist), new DoubleWritable(stream));
            }
            
        }
        
    }

    // Reducer of Job2 {Input: Artist, TotalStreams }
    public static class Spotify_Top20Reducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {
        
        private int n; 
        private TreeMap<Double, String> stream_list; 

        
        // Called once at the start of the task
        @Override
        public void setup(Reducer.Context context) {
            n = 20;  
            
            // list with artists gloabally sorted by their streaming counts
            stream_list = new TreeMap<Double, String>();
        }

        // This method is called once for each key
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) {
            
            double streamcount = 0;

            // get the streams for each artist
            for(DoubleWritable value : values){
                streamcount = value.get();
            }
                
            // adds the stream count as key and the artist as value for sorting based on the stream count
            // TreeMap sorts based on the keys
            stream_list.put(streamcount, key.toString());

            // checks if the global stream_list contains more than 20 elements
            if (stream_list.size() > n){
                
                // removing the first element with the smallest streaming count value
                stream_list.remove(stream_list.firstKey());
            }
                
        }

        // Called once at the end of the task
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException
        {
              
            // HashMap for sorting the stream count in reverse order
            LinkedHashMap<String, Double> streams_list_sorted = new LinkedHashMap<>();
            
            stream_list.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey(Comparator.reverseOrder()))
                    .forEachOrdered(x -> streams_list_sorted.put( x.getValue(), x.getKey()));
             
            // the output will be sorted by the reverse streaming count
            for (Map.Entry< String, Double> entry : streams_list_sorted.entrySet()) {
                
                String artist = entry.getKey();
                Double stream = entry.getValue();
                
                // the output will be consist only the top 20 artists
                context.write( NullWritable.get(), new Text(artist));
                
            }
            
        }
        
    }

    
    // Main method
    public static void main (String[] args) throws Exception  {
        
        Configuration conf = new Configuration();
        
        // Job1
        Job job1 = Job.getInstance(conf, "Sum of total streams based on regions");
        job1.setJarByClass(Top10StreamingRegions.class);
        
        job1.setMapperClass(Spotify_Mapper.class);
        job1.setReducerClass(Spotify_Reducer.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
    
        boolean job1_success = job1.waitForCompletion(true);
        
        if(job1_success){

            // Job2 
            Job job2 = Job.getInstance(conf, "Top 20 Global Music Artists based on streams");
            job2.setJarByClass(Top10StreamingRegions.class);

            job2.setMapperClass(Spotify_Top20Mapper.class);
            job2.setReducerClass(Spotify_Top20Reducer.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(DoubleWritable.class);

            job2.setOutputKeyClass(DoubleWritable.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(args[2]));
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            job2.waitForCompletion(true);
            
        }
        
    }
    
}
