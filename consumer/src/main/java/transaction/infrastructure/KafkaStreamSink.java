package transaction.infrastructure;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import transaction.dto.Fraud;
import transaction.dto.FraudSerializer;

public class KafkaStreamSink {

    public static KafkaSink<Fraud> kafkaSink(
            String kafkaAddress,
            String alarmTopic
    ) {
        return KafkaSink.<Fraud>builder()
                .setBootstrapServers(kafkaAddress)
                .setRecordSerializer(buildAlarmSerializer(alarmTopic))
                .build();
    }

    private static KafkaRecordSerializationSchema<Fraud> buildAlarmSerializer(String alarmTopic) {
        return KafkaRecordSerializationSchema.builder()
                .setTopic(alarmTopic)
                .setValueSerializationSchema(new FraudSerializer())
                .build();
    }
}
