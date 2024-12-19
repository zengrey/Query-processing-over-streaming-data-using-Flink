package org.example;

import org.apache.flink.streaming.api.CheckpointingMode;
/**
 * Hello world!
 *
 */
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class App 
{
      public static void main(String[] args) throws Exception {
        System.out.println("Starting TPC-H Query 3 Execution...");
        // Set up the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        TPCHQuery3CSV.execute(env);

    }
}
