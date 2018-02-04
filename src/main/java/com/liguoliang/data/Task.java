package com.liguoliang.data;

import org.apache.spark.sql.SparkSession;

public class Task {
    SparkSession sparkSession;

    public Task(SparkSession session) {
        sparkSession = session;
    }
}
