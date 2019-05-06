package com.gonnect.spring.taskaprk.wordcount;

import org.apache.camel.component.spark.VoidRddCallback;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class SerializableVoidRddCallback extends VoidRddCallback implements Serializable {

    @Override
    public void doOnRdd(JavaRDDLike rdd, Object... payloads) {
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
        // Save the word count back out to a text file, causing evaluation.
        counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });

    }
}
