package temperature;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class MeasurementDeserializer implements DeserializationSchema<Measurement> {

    private static final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new ParameterNamesModule())
            .addModule(new Jdk8Module())
            .addModule(new JavaTimeModule())
            .build();

    @Override
    public Measurement deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Measurement.class);
    }

    @Override
    public boolean isEndOfStream(Measurement nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Measurement> getProducedType() {
        return TypeInformation.of(Measurement.class);
    }
}