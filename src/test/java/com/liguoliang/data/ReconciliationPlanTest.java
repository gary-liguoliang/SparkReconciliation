package com.liguoliang.data;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class ReconciliationPlanTest {
    private ReconciliationPlan testingObj;


    @Test
    void generateSQL() {
        testingObj = new ReconciliationPlan("USERS", "INNER JOIN", "USERS_COPY",
                Arrays.asList("ID"), Arrays.asList("Id"));

        String sql = testingObj.withCheckKeys(Arrays.asList("NAME", "CREATED_ON"), Arrays.asList("Name", "CreatedOn")).generateSQL();

        assertEquals("SELECT * FROM USERS l INNER JOIN r USERS_COPY ON ( (l.ID = r.Id) )", sql);
    }

}