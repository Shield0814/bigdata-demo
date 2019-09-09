package com.ljl.flink.batch.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Arrays;

public class DataSetDataSourceAppJava {

    public static void main(String[] args) {
        ExecutionEnvironment jenv = ExecutionEnvironment.getExecutionEnvironment();

        jenv.fromCollection(Arrays.asList(
                "东方未明,男,28",
                "古实,男,26",
                "沈湘云,女,24"
        ));
    }

}
