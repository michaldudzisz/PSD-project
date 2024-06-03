package transaction;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import transaction.dto.Fraud;
import transaction.dto.Transaction;

import static transaction.anomalydetectors.LowValuesDetector.lowValuesDetector;
import static transaction.anomalydetectors.ValueAboveLimitDetector.valueAboveLimitDetector;
import static transaction.infrastructure.KafkaStreamSink.kafkaSink;
import static transaction.infrastructure.KafkaStreamSource.getDataStream;

public class TransactionConsumerJob {

    static final String KAFKA_ADDRESS = "kafka:29092";
    static final String SOURCE_TOPIC = "Transactions";
    static final String ALARM_TOPIC = "Frauds";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> dataStream = getDataStream(env, KAFKA_ADDRESS, SOURCE_TOPIC);
        KafkaSink<Fraud> alarmKafkaSink = kafkaSink(KAFKA_ADDRESS, ALARM_TOPIC);

        valueAboveLimitDetector(dataStream)
                .union(lowValuesDetector(dataStream))
                .union(lowValuesDetector(dataStream))
                .sinkTo(alarmKafkaSink);

        env.execute("Fraud detection alarm");
    }

}
