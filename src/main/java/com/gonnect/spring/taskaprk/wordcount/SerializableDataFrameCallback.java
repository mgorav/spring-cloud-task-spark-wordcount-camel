package com.gonnect.spring.taskaprk.wordcount;

import org.apache.camel.component.spark.DataFrameCallback;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

import static org.apache.spark.sql.functions.count;

public class SerializableDataFrameCallback implements Serializable, DataFrameCallback<String> {
    @Override
    public String onDataFrame(Dataset<Row> lines, Object... payloads) {
        Dataset<Row> wordCountsDataFrame = lines.groupBy("value").agg(count("value").as("total"));

        StringBuilder output = new StringBuilder();
        wordCountsDataFrame.collectAsList().stream().forEach(row -> {
            output.append(row + "\n");
        });


        return output.toString();
    }
}
