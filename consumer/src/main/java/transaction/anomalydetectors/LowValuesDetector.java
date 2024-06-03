package transaction.anomalydetectors;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.Collector;
import transaction.dto.Fraud;
import transaction.dto.Transaction;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ValueAboveLimitDetector implements AggregateFunction<Transaction, List<Transaction>, List<Fraud>> {

    public static DataStream<Fraud> valueAboveLimitDetector(DataStream<Transaction> dataStream) {
        return dataStream.keyBy(Transaction::getUserId)
                .window(SlidingEventTimeWindows.of(Duration.ofHours(24), Duration.ofMinutes(30)))
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
        System.out.println("Wondering if add transaction: " + transaction);
        if (transaction.isAboveLimit()) {
            System.out.println("Adding transaction: " + transaction);
            accumulator.add(transaction);
        }
        return accumulator;
    }

    @Override
    public List<Fraud> getResult(List<Transaction> accumulator) {
        System.out.println("Asked for results, current results: " + accumulator);
        return accumulator.stream()
                .filter(transaction -> Duration.between(transaction.getTimestamp(), LocalDateTime.now()).compareTo(Duration.ofSeconds(30)) <= 0)
                .map(transaction -> new Fraud(transaction, "Transaction above limit repeated within 24 hours"))
                .collect(Collectors.toList());
    }

    @Override
    public List<Transaction> merge(List<Transaction> a, List<Transaction> b) {
        a.addAll(b);
        return a;
    }
}
