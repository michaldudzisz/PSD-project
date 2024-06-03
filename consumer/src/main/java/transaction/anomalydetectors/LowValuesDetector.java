package transaction.anomalydetectors;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.Collector;
import transaction.dto.Fraud;
import transaction.dto.Transaction;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Comparator.*;

public class LowValuesDetector implements AggregateFunction<Transaction, List<Transaction>, List<Fraud>> {

    private static final BigDecimal minimalValue = BigDecimal.valueOf(1.0);

    private static final Duration windowLength = Duration.ofHours(7 * 24);          // 7 days
    private static final Duration windowSlide = Duration.ofHours((int) (3.5 * 24)); // 3.5 days

    public static DataStream<Fraud> lowValuesDetector(DataStream<Transaction> dataStream) {
        return dataStream.keyBy(Transaction::getCardId)
                .window(SlidingEventTimeWindows.of(windowLength, windowSlide))
                .aggregate(new LowValuesDetector())
                .flatMap((List<Fraud> frauds, Collector<Fraud> out) -> frauds.forEach(out::collect))
                .returns(TypeInformation.of(Fraud.class));
    }

    @Override
    public List<Transaction> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Transaction> add(Transaction transaction, List<Transaction> accumulator) {
        accumulator.add(transaction);
        return accumulator;
    }

    @Override
    public List<Fraud> getResult(List<Transaction> accumulator) {
        if (accumulator.isEmpty())
            return Collections.emptyList();

        List<Transaction> sortedTransactions = accumulator.stream()
                .sorted(comparing(Transaction::getValue))
                .collect(Collectors.toList());

        int p05Index = (int) (sortedTransactions.size() * 0.05);
        Transaction p05Transaction = sortedTransactions.get(p05Index);

        if (p05Transaction.getValue().compareTo(minimalValue) < 0) {
            return accumulator.stream()
                    .filter(transaction -> transaction.getValue().compareTo(minimalValue) < 0)
                    .map(transaction -> new Fraud(transaction, "5th percentile lower than " + minimalValue))
                    .collect(Collectors.toList());
        }

        return Collections.emptyList();
    }

    @Override
    public List<Transaction> merge(List<Transaction> a, List<Transaction> b) {
        a.addAll(b);
        return a;
    }
}
