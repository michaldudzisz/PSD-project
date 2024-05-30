package temperature;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TemperatureConsumerJob {

    static final String KAFKA_ADDRESS = "kafka:29092";
    static final String SOURCE_TOPIC = "Temperature";
    static final String ALARM_TOPIC = "Alarm";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Measurement> dataStream = getDataStream(env);
        KafkaSink<Alarm> alarmKafkaSink = kafkaSink();

        dataStream.filter(Measurement::isBelowZero)
                .map(Alarm::fromMeasurement)
                .sinkTo(alarmKafkaSink);

        env.execute("Temperature measurements alarm");
    }


    private static DataStream<Measurement> getDataStream(StreamExecutionEnvironment env) {
        KafkaSource<Measurement> kafkaSource = getKafkaSource();
        return env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }

    private static KafkaSource<Measurement> getKafkaSource() {
        return KafkaSource.<Measurement>builder()
                .setBootstrapServers(KAFKA_ADDRESS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new MeasurementDeserializer())
                .build();
    }

    private static KafkaSink<Alarm> kafkaSink() {
        return KafkaSink.<Alarm>builder()
                .setBootstrapServers(KAFKA_ADDRESS)
                .setRecordSerializer(buildAlarmSerializer())
                .build();
    }

    private static KafkaRecordSerializationSchema<Alarm> buildAlarmSerializer() {
        return KafkaRecordSerializationSchema.builder()
                .setTopic(ALARM_TOPIC)
                .setValueSerializationSchema(new AlarmSerializer())
                .build();
    }
}
