package com.liguoliang.data;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Date;

import static org.junit.jupiter.api.Assertions.*;

class ETLTaskTest {

    public static final String USERS_CSV = "users_csv";
    public static final String USERS_COPY_CSV = "users_copy_csv";
    private static ETLTask testingApp;
    private static SparkSession sparkSession;
    private static Dataset<Row> usersLoaded = null;
    private static Dataset<Row> usersCopyLoaded = null;

    @BeforeAll
    static void setUp() throws AnalysisException {
        sparkSession = SparkSession.builder().appName("test").master("local[4]").getOrCreate();
        testingApp = new ETLTask(sparkSession);

        usersLoaded = testingApp.loadCSV("src/test/resources/testdata/users.csv", true, USERS_CSV);
        usersCopyLoaded = testingApp.loadCSV("src/test/resources/testdata/users-copy.csv", true, USERS_COPY_CSV);

    }

    @AfterAll
    static void tearDown() {
        sparkSession.close();
    }


    @Test
    void loadCSV() throws Exception {
//        Dataset<Row> usersLoaded = testingApp.loadCSV(sparkSession, "src/test/resources/testdata/users.csv", true, "USERS");

//        Dataset<Row> usersLoaded = sparkSession.sql("SELECT ID, NAME, " + // " CAST(ID as INT) as ID, NAME, " +
//                "TO_DATE(CAST(UNIX_TIMESTAMP(CreatedOn, 'yy/MM/dd hh:mm:ss') AS TIMESTAMP)) as D," +
//                "FROM_UNIXTIME(UNIX_TIMESTAMP(CreatedOn, 'yy/MM/dd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss') as D2, " +
//                "FROM_UTC_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(CreatedOn, 'yy/MM/dd hh:mm:ss'), 'yyyy-MM-dd hh:mm:ss'), 'UTC') as D3 " +
//                "FROM USERS");


        System.out.println(usersLoaded.first());
        usersLoaded.printSchema();

        Row first = usersLoaded.first();
        assertEquals("1", first.get(0));
        assertEquals("Jane", first.get(1));
        assertEquals("18/02/04 09:43:05", first.get(2));
//        assertEquals(Timestamp.valueOf("2018-02-04 09:43:05"), Timestamp.valueOf(first.getString(3)));
//        assertEquals(Timestamp.valueOf("2018-02-04 09:43:05"), first.get(4));
    }

    @Test
    void selectTo() throws Exception {
        String sqlSelect = "SELECT CAST(ID as INT) as ID, NAME, " +
                "TO_DATE(CAST(UNIX_TIMESTAMP(CreatedOn, 'yy/MM/dd hh:mm:ss') AS TIMESTAMP)) as CreatedOn  " +
                "FROM " + USERS_CSV;
        Dataset<Row> refinedDataset = testingApp.selectInTo(sqlSelect, USERS_CSV + "_refined");
        refinedDataset.printSchema();
        Row first = refinedDataset.first();
        assertEquals(1, first.get(0));
        assertEquals("Jane", first.get(1));
        assertEquals(Date.valueOf("2018-02-04"), first.get(2));
    }

    @Test
    void exportToCSV() throws Exception {
        String sql = "SELECT CAST(ID as INT) as ID, NAME, " +
                "TO_DATE(CAST(UNIX_TIMESTAMP(CreatedOn, 'yy/MM/dd hh:mm:ss') AS TIMESTAMP)) as CreatedOn  " +
                "FROM " + USERS_CSV;
        testingApp.exportToCSV(sql, "target/out.csv");
    }

    @Test
    void performJoin() throws Exception {
        String sqlJoin = "SELECT * FROM " + USERS_CSV + " users INNER JOIN " + USERS_COPY_CSV + " users_copy ON users.ID = users_copy.ID AND users.NAME = users_copy.NAME";
        Dataset<Row> result = testingApp.selectInTo(sqlJoin, "joinResult");
        result.printSchema();
        System.out.println(result.first().mkString());
        System.out.println("matched count: " + result.count());

        sqlJoin = "SELECT * FROM " + USERS_CSV + " users INNER JOIN " + USERS_COPY_CSV + " users_copy ON users.ID = users_copy.ID WHERE (users.NAME != users_copy.NAME)";
        Dataset<Row> r = testingApp.selectInTo(sqlJoin, "joinResult2");
        System.out.println("breaks count: " + r.count());

        sqlJoin = "SELECT * FROM " + USERS_CSV + " users LEFT OUTER JOIN " + USERS_COPY_CSV + " users_copy on users.ID = users_copy.ID WHERE users_copy.ID IS NULL";
        r = testingApp.selectInTo(sqlJoin, "breaks_left");
        System.out.println("breaks count: " + r.count());
    }

}