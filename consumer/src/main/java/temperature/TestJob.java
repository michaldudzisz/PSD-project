//package temperature;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
//import org.apache.flink.connector.kafka.sink.KafkaSink;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.util.Collector;
//
//import java.time.Duration;
//import java.time.LocalDateTime;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.stream.Collectors;
//
//public class TemperatureConsumerJob {
//
//    static final String KAFKA_ADDRESS = "kafka:29092";
//    static final String SOURCE_TOPIC = "Transactions";
//    static final String ALARM_TOPIC = "Alarm";
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStream<Transaction> dataStream = getDataStream(env);
//        KafkaSink<Fraud> alarmKafkaSink = kafkaSink();
//
//        dataStream
//                .keyBy(Transaction::getUserId)
//                .process(new KeyedProcessFunction<Integer, Transaction, Fraud>() {
//                    private transient ValueState<List<Transaction>> transactionAboveLimitWithin24h;
//
//                    @Override
//                    public void open(Configuration parameters) {
//                        ValueStateDescriptor<List<Transaction>> transactionAboveLimitWithin24hDescriptor =
//                                new ValueStateDescriptor<>(
//                                        "transactionAboveLimitWithin24h",
//                                        TypeInformation.of(new TypeHint<List<Transaction>>() {})
//                                );
//                        transactionAboveLimitWithin24h = getRuntimeContext().getState(transactionAboveLimitWithin24hDescriptor);
//                    }
//
//                    @Override
//                    public void processElement(Transaction transaction, Context ctx, Collector<Fraud> out) throws Exception {
//                        System.out.println("jestem w process elements: " + transaction);
//                        LocalDateTime occurrenceTime = transaction.getTimestamp();
//                        System.out.println("transactionAboveLimitWithin24h.value() : " + transactionAboveLimitWithin24h.value());
//                        List<Transaction> transactionsAboveLimitWithin24hFromState = transactionAboveLimitWithin24h.value() == null ? new ArrayList<>() : transactionAboveLimitWithin24h.value();
//                        System.out.println("transactionsAboveLimitWithin24hFromState: " + transactionsAboveLimitWithin24hFromState);
//                        List<Transaction> transactionsAboveLimitWithin24hLocal = transactionsAboveLimitWithin24hFromState.stream()
//                                .filter(elem -> Duration.between(elem.getTimestamp(), occurrenceTime).compareTo(Duration.ofHours(24)) <= 0)
//                                .collect(Collectors.toList());
//                        transactionAboveLimitWithin24h.update(transactionsAboveLimitWithin24hLocal);
//                        System.out.println("transactionsAboveLimitWithin24hLocal: " + transactionsAboveLimitWithin24hLocal);
//
//                        if (transaction.isAboveLimit()) {
//                            System.out.println("Mam above limit: " + transaction);
//                            transactionsAboveLimitWithin24hLocal.add(transaction);
//                            System.out.println("transactionsAboveLimitWithin24hLocal: " + transactionsAboveLimitWithin24hLocal);
//                            transactionAboveLimitWithin24h.update(transactionsAboveLimitWithin24hLocal);
//
//                            if (transactionsAboveLimitWithin24hLocal.size() >= 2) {
//                                Fraud fraud = new Fraud(transaction, "Użytkownik " + transaction.getUserId() + " przekroczył limit co najmniej drugi raz w ciągu 24 godzin.");
//                                out.collect(fraud);
//                                System.out.println("Mam frauda: " + fraud);
//                            }
//                        }
//                    }
//                })
//                .sinkTo(alarmKafkaSink);
//
//        env.execute("Fraud detection alarm");
//    }
//
//
//    private static DataStream<Transaction> getDataStream(StreamExecutionEnvironment env) {
//        KafkaSource<Transaction> kafkaSource = getKafkaSource();
//        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
//    }
//
//    private static KafkaSource<Transaction> getKafkaSource() {
//        return KafkaSource.<Transaction>builder()
//                .setBootstrapServers(KAFKA_ADDRESS)
//                .setTopics(SOURCE_TOPIC)
//                .setGroupId("flink-consumer-group")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new TransactionDeserializer())
//                .build();
//    }
//
//    private static KafkaSink<Fraud> kafkaSink() {
//        return KafkaSink.<Fraud>builder()
//                .setBootstrapServers(KAFKA_ADDRESS)
//                .setRecordSerializer(buildAlarmSerializer())
//                .build();
//    }
//
//    private static KafkaRecordSerializationSchema<Fraud> buildAlarmSerializer() {
//        return KafkaRecordSerializationSchema.builder()
//                .setTopic(ALARM_TOPIC)
//                .setValueSerializationSchema(new FraudSerializer())
//                .build();
//    }
//}
