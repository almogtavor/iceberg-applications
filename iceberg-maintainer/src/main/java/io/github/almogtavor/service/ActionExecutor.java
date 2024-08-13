package io.github.almogtavor.service;

import org.apache.spark.sql.SparkSession;

public interface ActionExecutor {
    void execute(SparkSession spark);
}
