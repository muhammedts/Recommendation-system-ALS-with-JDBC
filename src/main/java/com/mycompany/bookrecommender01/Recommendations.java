/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.bookrecommender01;

/**
 *
 * @author MuhammedTarek
 */
public class Recommendations {
        private int userId;
        private int Recommendation1;
          private int Recommendation2;
        public int getRecommendation1() {
        return Recommendation1;
    }

    public void setRecommendation1(int Recommendation) {
        this.Recommendation1 = Recommendation;
    }
          public int getRecommendation2() {
        return Recommendation2;
    }

    public void setRecommendation2(int Recommendation) {
        this.Recommendation2 = Recommendation;
    }
              public int getuserId() {
        return userId;
    }

    public void setuserId(int Recommendation) {
        this.userId = Recommendation;
    }
}


