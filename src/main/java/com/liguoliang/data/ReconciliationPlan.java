package com.liguoliang.data;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class ReconciliationPlan {

    private String leftTable;
    private String rightTable;
    private String joinType;
    private List<String> joinByKeyFromLeft;
    private List<String> joinByKeyFromRight;
    List<Tuple2<String, String>> joinOnKeys;
    private List<String> checkKeysFromLeft;
    private List<String> checkKeysFromRight;

    public ReconciliationPlan(String leftTable, String joinType, String rightTable,
                              List<String> joinByKeyFromLeft, List<String> joinByKeyFromRight) {
        if (joinByKeyFromLeft.size() != joinByKeyFromRight.size()) {
            throw new RuntimeException("Keys used in join have different size");
        }

        this.leftTable = leftTable;
        this.joinType = joinType;
        this.rightTable = rightTable;
//        this.joinByKeyFromLeft = joinByKeyFromLeft;
//        this.joinByKeyFromRight = joinByKeyFromRight;
        joinOnKeys = new ArrayList<Tuple2<String, String>>();
        for (int i = 0; i < joinByKeyFromLeft.size(); i++) {
            joinOnKeys.add(new Tuple2<String, String>(joinByKeyFromLeft.get(i), joinByKeyFromRight.get(i)));
        }
    }

    public ReconciliationPlan withCheckKeys(List<String> keysFromLeft, List<String> keysFromRight) {
        this.checkKeysFromLeft = keysFromLeft;
        this.checkKeysFromRight = keysFromRight;
        return this;
    }

    public String generateSQL() {
        StringBuilder onConditions = new StringBuilder();
        for (Tuple2<String, String> joinOnKey : joinOnKeys) {
            if(onConditions.length() > 0) {
                onConditions.append(" AND ");
            }
            onConditions.append(String.format(" (l.%s = r.%s) ", joinOnKey._1, joinOnKey._2));
        }
        String sql = String.format("SELECT * FROM %s l %s r %s ON (%s)", leftTable, joinType, rightTable, onConditions);

        return sql;
       // long perfectMatch = sparkSession.sql(sql).count();
       // System.out.println("perfectMatch: " + perfectMatch);
    }


}
