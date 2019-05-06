package com.gonnect.spring.taskaprk.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

@SpringBootApplication
@EnableTask
public class SpringCloudTaskWordcountApplication implements Serializable {

    @Bean
    public JavaSparkContext javaSparkContext() {
        SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc;
    }

    @Bean
    public SparkSession sparkSession() {
        return SparkSession
                .builder().appName("wordcount").master("local[*]").getOrCreate();
    }

    @Bean
    public CommandLineRunner commandLineRunner() {
        return new SparkWordCountCommandLineRunner();
    }


    @Bean
    Dataset<String> fileDataFrame(SparkSession sparkSession, @Value("classpath:testrdd.txt") Resource resource) throws IOException {
        return sparkSession.read().textFile(resource.getURI().getPath());
    }


    @Bean
    JavaRDDLike<String, JavaRDD<String>> myRdd(JavaSparkContext sparkContext, @Value("classpath:testrdd.txt") Resource resource) throws IOException {
        return sparkContext.textFile(resource.getURI().getPath());
    }

    @Bean
    SerializableCountWordRddCallback count() {
        return new SerializableCountWordRddCallback();
    }


    JavaRDDLike<String, JavaRDD<String>> lines(JavaRDDLike<String, JavaRDD<String>> myRdd) throws IOException {
        return myRdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\n")).iterator();
            }
        });
    }


    public static void main(String[] args) {
        SpringApplication.run(SpringCloudTaskWordcountApplication.class, args);
    }


}
