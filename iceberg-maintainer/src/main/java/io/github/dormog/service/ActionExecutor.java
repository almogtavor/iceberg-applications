package io.github.dormog.service;

import org.apache.spark.sql.SparkSession;

public interface ActionExecutor {
    void execute(SparkSession spark);
}
