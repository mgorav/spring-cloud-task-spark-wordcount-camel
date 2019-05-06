package com.gonnect.spring.taskaprk.wordcount;

import org.apache.camel.component.spark.RddCallback;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Consumer;

public class SerializableCountWordRddCallback implements RddCallback<String>, Serializable {


    @Override
    public String onRdd(JavaRDDLike rdd, Object... payloads) {

        JavaRDDLike words = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\n")).iterator();
            }
        });

        // Transform into word and count.
        JavaPairRDD<String, Integer> counts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String x) {
                        return new Tuple2(x, 1);
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });

        StringBuilder output = new StringBuilder();
        // Save the word count back out to a text file, causing evaluation.

        counts.collect().stream().forEach(new Consumer<Tuple2<String, Integer>>() {
            @Override
            public void accept(Tuple2<String, Integer> tuple2) {
                output.append(tuple2 + "\n");
            }
        });


        return output.toString();
    }
}
