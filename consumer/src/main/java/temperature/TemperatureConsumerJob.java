package temperature;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.Collector;
import sun.awt.X11.XTranslateCoordinates;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TemperatureConsumerJob {

    static final String KAFKA_ADDRESS = "kafka:29092";
    static final String SOURCE_TOPIC = "Transactions";
    static final String ALARM_TOPIC = "Frauds";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> dataStream = getDataStream(env);
        KafkaSink<Fraud> alarmKafkaSink = kafkaSink();

        DataStream<List<Fraud>> stream = dataStream
                .keyBy(Transaction::getUserId)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(90), Duration.ofSeconds(30)))
                .aggregate(new ValueAboveLimitDetector());

        stream.print();

        DataStream<Fraud> aggregatedStream = stream.flatMap((List<Fraud> frauds, Collector<Fraud> out) -> frauds.forEach(out::collect))
                .returns(TypeInformation.of(Fraud.class));

        aggregatedStream.print();

        aggregatedStream.sinkTo(alarmKafkaSink);

        env.execute("Fraud detection alarm");
    }

    public static class ValueAboveLimitDetector implements AggregateFunction<Transaction, List<Transaction>, List<Fraud>> {

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


    private static DataStream<Transaction> getDataStream(StreamExecutionEnvironment env) {
        KafkaSource<Transaction> kafkaSource = getKafkaSource();
        return env.fromSource(kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
    }

    private static KafkaSource<Transaction> getKafkaSource() {
        return KafkaSource.<Transaction>builder()
                .setBootstrapServers(KAFKA_ADDRESS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();
    }

    private static KafkaSink<Fraud> kafkaSink() {
        return KafkaSink.<Fraud>builder()
                .setBootstrapServers(KAFKA_ADDRESS)
                .setRecordSerializer(buildAlarmSerializer())
                .build();
    }

    private static KafkaRecordSerializationSchema<Fraud> buildAlarmSerializer() {
        return KafkaRecordSerializationSchema.builder()
                .setTopic(ALARM_TOPIC)
                .setValueSerializationSchema(new FraudSerializer())
                .build();
    }
}
