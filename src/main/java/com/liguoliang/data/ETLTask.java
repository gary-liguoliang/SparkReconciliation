package com.liguoliang.data;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ETLTask extends Task {

    public ETLTask(SparkSession session) {
        super(session);
    }

    //    public SparkSession getSparkSession() {
//        return SparkSession.builder().appName("test").master("local[4]").getOrCreate();
//    }

    public Dataset<Row> loadCSV(String csvPath, boolean hasHeader, String viewName) throws AnalysisException {
        Dataset<Row> dataset = sparkSession.read().
                option("header", hasHeader).option("inferSchema", false).
                csv(csvPath);
        dataset.createTempView(viewName);
        return dataset;
    }


    public Dataset<Row> selectInTo(String sqlSelect, String viewName) throws AnalysisException {
        Dataset<Row> result = sparkSession.sql(sqlSelect);
        result.createTempView(viewName);
        return result;
    }

    public void exportToCSV(String sql, String destination) {
        sparkSession.sql(sql).repartition(1).coalesce(1).write().option("header", true).mode("overwrite").csv(destination);
    }

//    public void join(SparkSession sparkSession, String left, String joinType, String right, joinn)
}
