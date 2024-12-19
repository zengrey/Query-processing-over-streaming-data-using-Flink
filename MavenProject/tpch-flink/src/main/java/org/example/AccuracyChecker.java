package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Set;

public class AccuracyChecker extends KeyedProcessFunction<Integer, String, Tuple2<String, Double>> {
    private transient ValueState<Long> correctCount;
    private transient ValueState<Long> totalCount;
    private final Set<String> referenceSet;

    public AccuracyChecker(Set<String> referenceSet) {
        this.referenceSet = referenceSet;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        correctCount = getRuntimeContext().getState(new ValueStateDescriptor<>("correctCount", Long.class, 0L));
        totalCount = getRuntimeContext().getState(new ValueStateDescriptor<>("totalCount", Long.class, 0L));
    }

    @Override
    public void processElement(String value, Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
        long total = totalCount.value() + 1;
        totalCount.update(total);

        if (referenceSet.contains(value)) {
            long correct = correctCount.value() + 1;
            correctCount.update(correct);
        }

        double accuracy = ((double) correctCount.value() / total) * 100;
        out.collect(new Tuple2<>(value, accuracy));
    }
}


