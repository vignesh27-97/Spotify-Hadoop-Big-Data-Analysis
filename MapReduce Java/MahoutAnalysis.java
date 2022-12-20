/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Other/File.java to edit this template
 */
package com.mycompany.finalproject_spotifycharts;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

/**
 *
 * @author vignesh
 */
public class MahoutAnalysis {

    public static  void main(String[] args) throws Exception{
        
        String pathFile = "/Users/vignesh/VK/DAMG/BigDataEngineering/Project/Dataset/RankStreams.csv";
        
        File data = new File(pathFile);
        
        String[] values = data.toString().split("\n", -1);
        
        HashMap<Integer, Integer> rankStreams = new HashMap<Integer, Integer>();
        
        for(String val: values){
        
            String[] tokens = val.split(",", -1);
            int temp1;
            int temp2;
            
            if(tokens[0].equals("")){
                temp1 = 0;
            }else{
                temp1 = Integer.parseInt(tokens[0]);
            }
            
            if(tokens[1].equals("")){
                temp2 = 0;
            }else{
                temp2 = Integer.parseInt(tokens[1]);
            }
            
            rankStreams.put(temp1, temp2);
            
        }
        
//        DataModel model = new FileDataModel(pathFile);
//        
//        UserSimilarity similarity= new PearsonCorrelationSimilarity (model);
//        
//        UserNeighborhood neighborhood = new NearestNUserNeighborhood (2, similarity, model);
//        
//        Recommender recommender = new GenericUserBasedRecommender (model, neighborhood, similarity);
//        
//        List<RecommendedItem> recommendations = recommender. recommend (1, 1);
//
//        for (RecommendedItem recommendation : recommendations){
//            System.out.println (recommendation);
//        }
        
    }
    
}

