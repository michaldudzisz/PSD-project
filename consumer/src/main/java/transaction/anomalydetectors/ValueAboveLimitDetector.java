package transaction.anomalydetectors;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.Collector;
import transaction.dto.Fraud;
import transaction.dto.Transaction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ValueAboveLimitDetector implements AggregateFunction<Transaction, List<Transaction>, List<Fraud>> {

    private static final Duration windowLength = Duration.ofHours(24);
    private static final Duration windowSlide = Duration.ofHours(12);
    private static final int numberOfAllowedTransactionAboveLimitInWindow = 2;


    public static DataStream<Fraud> valueAboveLimitDetector(DataStream<Transaction> dataStream) {
        return dataStream.keyBy(Transaction::getUserId)
                .window(SlidingEventTimeWindows.of(windowLength, windowSlide))
                .aggregate(new ValueAboveLimitDetector())
                .flatMap((List<Fraud> frauds, Collector<Fraud> out) -> frauds.forEach(out::collect))
                .returns(TypeInformation.of(Fraud.class));
    }

    @Override
    public List<Transaction> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Transaction> add(Transaction transaction, List<Transaction> accumulator) {
        if (transaction.isAboveLimit()) {
            accumulator.add(transaction);
        }
        return accumulator;
    }

    @Override
    public List<Fraud> getResult(List<Transaction> accumulator) {
        if (accumulator.size() <= numberOfAllowedTransactionAboveLimitInWindow)
            return Collections.emptyList();

        List<Transaction> frauds = new ArrayList<>(accumulator.subList(3, accumulator.size()));
        return frauds.stream()
                .map(transaction -> new Fraud(transaction, "Transaction above limit repeated within 24 hours"))
                .collect(Collectors.toList());
    }

    @Override
    public List<Transaction> merge(List<Transaction> a, List<Transaction> b) {
        a.addAll(b);
        return a;
    }
}
