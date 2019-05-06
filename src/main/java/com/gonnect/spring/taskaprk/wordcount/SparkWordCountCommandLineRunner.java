package com.gonnect.spring.taskaprk.wordcount;

import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;

import java.io.Serializable;

import static org.apache.camel.component.spark.SparkConstants.SPARK_DATAFRAME_CALLBACK_HEADER;
import static org.apache.camel.component.spark.SparkConstants.SPARK_RDD_CALLBACK_HEADER;

public class SparkWordCountCommandLineRunner implements CommandLineRunner, Serializable {
    @EndpointInject
    private ProducerTemplate producer;

    @Autowired
    private SparkSession sparkSession;

    @Override
    public void run(String... strings) throws Exception {

        String sparkUri1 = "spark:rdd?rdd=#myRdd";
        String sparkUri2 = "spark:rdd?rdd=#myRdd&rddCallback=#count";
        String sparkDataFrameUri = "spark:dataFrame?dataFrame=#fileDataFrame";


        producer.sendBodyAndHeader(sparkUri1, null, SPARK_RDD_CALLBACK_HEADER, new SerializableVoidRddCallback());


        String output = producer.requestBody(sparkUri2, null, String.class);

        System.out.println(output);

        output = producer.requestBodyAndHeader(sparkDataFrameUri, null, SPARK_DATAFRAME_CALLBACK_HEADER, new SerializableDataFrameCallback(), String.class);

        System.out.println(output);

    }
}