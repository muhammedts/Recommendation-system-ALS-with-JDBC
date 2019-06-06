/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.bookrecommender01;

import java.io.NotSerializableException;
import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;
import java.util.Iterator;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.spark.api.java.function.ForeachFunction;

/**
 *
 * @author Muhammed Tarek
 */
public class Model implements Serializable {
    public void Recommend(){
       SparkSession session = SparkSession.builder()
                .appName("BookRecommender")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///D://")
                .getOrCreate();
// reading from csv 
/*
        Dataset<Row> dataset = session
                .read().option("header", "true")
                .csv("ratings.csv");
        dataset.show();*/

        // reading from database   
        Dataset<Row> dataset = session.
                read()
                .option("url", "jdbc:mysql://localhost:3306/ratings")
                .option("user", "root")
                .option("password", "")
                .option("dbtable", "rates")
                .option("driver", "com.mysql.jdbc.Driver")
                .format("jdbc")
                .load();
        System.out.println(" looooooooooooooooooooooooooooooooooooooooooooooooooooooool");
        dataset.show();

      
        //preprocessing
        JavaRDD<Rating> javaRDD = dataset.toJavaRDD().map(new Function<Row, Rating>() {
            @Override
            public Rating call(Row row) throws Exception {
                //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                //   System.out.println("here " + row.toString());
                String[] fields = row.toString().split(",");
                //System.out.println("userid "+fields[0]);
                // System.err.println("ISBN "+fields[1]);

                // System.out.println("userid " + fields[2]);
                String userId = fields[0];
                int nuserId = Integer.parseInt(userId.replaceAll("[\\[\\]]", ""));
                String ISBN = fields[1];
                int nISBN = Integer.parseInt(ISBN.replaceAll("[\\[\\]]", ""));
                String rate = fields[2];
                int Ratee = Integer.parseInt(rate.replaceAll("[\\[\\]]", ""));
                Rating r = new Rating();
                r.setUserId(nuserId);
                r.setRate(Ratee);
                r.setISBN(nISBN);
                return r;
            }
        });

        Dataset<Row> ratingDataset = session.createDataFrame(javaRDD, Rating.class);
        //  ratingDataset.write().json("E://cvbb");
        ratingDataset.show();
   
        Dataset<Row>[] splits = ratingDataset.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];
        ALS als = new ALS().setMaxIter(10).setRegParam(0.01).setUserCol("userId").setItemCol("ISBN").setRatingCol("rate");
        ALSModel model = als.fit(training);
        model.setColdStartStrategy("drop");
        Dataset<Row> predictions = model.transform(test);
        Dataset<Row> r = model.recommendForAllUsers(10);
        System.out.println(" r show");
        r.show();
        // r.coalesce(1).write().json("output1");
        //r.write().json("jesondatasetalgoresult11");
     
        Properties b =new Properties();
        b.put("user", "root");
        b.put("password","");
        b.put("driver","com.mysql.jdbc.Driver");
    
      //  newuser.write().jdbc("jdbc:mysql://localhost:3306/rates", "alldata",b);
      //  newuser.show();
          System.out.println("HEREEEEEEEEEEEEEEEEEEEEEEE");
        //newuser.coalesce(1).write().json("recs");
          JavaRDD<Recommendations> javaRDD1 = r.toJavaRDD().map(new Function<Row, Recommendations>() {
            @Override
            public Recommendations call(Row row) throws Exception {
                //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                //   System.out.println("here " + row.toString());
                String[] fields = row.toString().split(",");
               
                String userId =fields[0];
               int nuserId =Integer.parseInt(userId.replaceAll("[\\[\\]]", "").replace("WrappedArray(", "").replaceAll("\"", "").replaceAll("\\s",""));
                 System.out.println(nuserId);

                String reco1 =fields[1];
               int nrec =Integer.parseInt(reco1.replaceAll("[\\[\\]]", "").replace("WrappedArray(", "").replaceAll("\"", "").replaceAll("\\s",""));               
             
                String reco2 =fields[3];
               int nrec2 =Integer.parseInt(reco2.replaceAll("[\\[\\]]", "").replaceAll("\"", "").replaceAll("\\s",""));
                  
               String reco3 =fields[5];
               int nrec3 =Integer.parseInt(reco3.replaceAll("[\\[\\]]", "").replaceAll("\"", "").replaceAll("\\s",""));
               
                   String reco4 =fields[7];
               int nrec4 =Integer.parseInt(reco4.replaceAll("[\\[\\]]", "").replaceAll("\"", "").replaceAll("\\s",""));
               
                   String reco5 =fields[9];
               int nrec5 =Integer.parseInt(reco5.replaceAll("[\\[\\]]", "").replaceAll("\"", "").replaceAll("\\s",""));
               
                   String reco6 =fields[11];
               int nrec6 =Integer.parseInt(reco6.replaceAll("[\\[\\]]", "").replaceAll("\"", "").replaceAll("\\s",""));
               
                   String reco7 =fields[13];
               int nrec7 =Integer.parseInt(reco7.replaceAll("[\\[\\]]", "").replaceAll("\"", "").replaceAll("\\s",""));
               
                   String reco8 =fields[15];
               int nrec8 =Integer.parseInt(reco8.replaceAll("[\\[\\]]", "").replaceAll("\"", "").replaceAll("\\s",""));
               
                   String reco9 =fields[17];
               int nrec9 =Integer.parseInt(reco9.replaceAll("[\\[\\]]", "").replaceAll("\"", "").replaceAll("\\s",""));
               
                   String reco10 =fields[19];
               int nrec10 =Integer.parseInt(reco10.replaceAll("[\\[\\]]", "").replaceAll("\"", "").replaceAll("\\s",""));
               
               
               
        Recommendations r = new Recommendations();
        r.setUserId(nuserId);
        r.setRecommendation1(nrec);
        r.setRecommendation2(nrec2);
        r.setRecommendation3(nrec3);
        r.setRecommendation4(nrec4);
        r.setRecommendation5(nrec5);
        r.setRecommendation6(nrec6);
        r.setRecommendation7(nrec7);
        r.setRecommendation8(nrec8);
        r.setRecommendation9(nrec9);
        r.setRecommendation10(nrec10);
        return r;
            }
        });
        Dataset<Row> RecommendationDataset = session.createDataFrame(javaRDD1, Recommendations.class);
        RecommendationDataset.show();
        //deleting table
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
              Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/ratings", "root", "");
           System.out.println("Conncted!!!");
          PreparedStatement stmnt = conn.prepareStatement("DROP TABLE Recommendations");
           stmnt.execute();
           stmnt.close();
        
        } catch (ClassNotFoundException | SQLException ex) {
            Logger.getLogger(Model.class.getName()).log(Level.SEVERE, null, ex);
        }
               RecommendationDataset.write().jdbc("jdbc:mysql://localhost:3306/ratings", "Recommendations",b);
     // evaluation crieteria for model
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rate")
                .setPredictionCol("prediction");
        Double rmse = evaluator.evaluate(predictions);
        System.out.println("Root-mean-square error = " + rmse);

    
    }

    public static void main(String[] args) throws NotSerializableException {
                int delay = 5000; //msecs
                Model obj =new Model();
         Timer timer = new Timer();
    timer.schedule(new TimerTask()
    {
        @Override
        public void run()
        {
     obj.Recommend();
        }
    }, delay,900000); 

     

   
    }
}

////                .csv("C:\\Users\\AbdrhmnAns\\Documents\\dataset1.csv")
  /*         .read().option("header", "true")
                .csv("output.csv");*/
        //dataset.show();
        //data.write().json("D:\\jsondata");
        ////Dataset<Row> mydata = session.read().json("D:\\jsondata\\m.json");
        ////mydata.show();
        /* Dataset<Row> dataset2 = session
                .read().option("header", "true")
                .csv("output.csv");*/
        //dataset.show();
        //data.write().json("D:\\jsondata");
        ////Dataset<Row> mydata = session.read().json("D:\\jsondata\\m.json");
        ////mydata.show();
     
//dframe = dframe.withColumn("c_number", dframe.col("c_a").cast("decimal(38,0)"));
        /*  StringIndexerModel userIdLI = new StringIndexer().setInputCol("userId").fit(ratingDataset).setOutputCol("ouserId");
        StringIndexerModel ISBNLI = new StringIndexer().setInputCol("ISBN").fit(ratingDataset).setOutputCol("oISBN");
        StringIndexerModel rateLI = new StringIndexer().setInputCol("rate").fit(ratingDataset).setOutputCol("orate");
        Pipeline p1 = new Pipeline().setStages(new PipelineStage[]{userIdLI, ISBNLI, rateLI});
        Dataset<Row> data = p1.fit(ratingDataset).transform(ratingDataset);
        
        System.out.println("heeeeeeeeeeeeeeeeeeeere");
        
        data.show();
         */

    
//        try {
//            //users.foreach((ForeachFunction<Row>) row -> System.out.println(row));
//            Class.forName("com.mysql.cj.jdbc.Driver");
//            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/rates", "root", "");
//            System.out.println("Conncted!!!");
//            PreparedStatement stmnt = conn.prepareStatement("INSERT INTO recommendations VALUES(?)");
//            
//            
//            
//            newuser.foreachPartition((Iterator<Row> t) -> {
//                while (t.hasNext()) {
//                    String x;
//                    Row row = t.next();
//                    x = row.toString();
//                    System.out.println(" here");
//                    System.out.println(x);
//                    stmnt.setString(1, x);
//                    stmnt.execute();
//                }
//                stmnt.close();
//            });
//        } catch (ClassNotFoundException | SQLException ex) {
//            Logger.getLogger(Model.class.getName()).log(Level.SEVERE, null, ex);
//        }
