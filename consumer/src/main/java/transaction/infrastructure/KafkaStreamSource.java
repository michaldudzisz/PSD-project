package transaction.infrastructure;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import transaction.dto.Transaction;
import transaction.dto.TransactionDeserializer;

public class KafkaStreamSource {

    public static DataStream<Transaction> getDataStream(
            StreamExecutionEnvironment env,
            String kafkaAddress,
            String sourceTopic
    ) {
        KafkaSource<Transaction> kafkaSource = getKafkaSource(kafkaAddress, sourceTopic);
        return env.fromSource(kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");
    }

    private static KafkaSource<Transaction> getKafkaSource(
            String kafkaAddress,
            String sourceTopic
    ) {
        return KafkaSource.<Transaction>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics(sourceTopic)
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();
    }
}
