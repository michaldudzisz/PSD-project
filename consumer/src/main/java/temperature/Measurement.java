package temperature;

import java.time.LocalDateTime;
import java.util.Objects;

public class Measurement {

    public int thermometerId;
    public LocalDateTime timestamp;
    public float temperature;

    public Measurement() {}

    public Measurement(int thermometerId, LocalDateTime timestamp, int temperature) {
        this.thermometerId = thermometerId;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public boolean isBelowZero() {
        return temperature < 0;
    }

    public int getThermometerId() {
        return thermometerId;
    }

    public void setThermometerId(int thermometerId) {
        this.thermometerId = thermometerId;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Measurement that = (Measurement) o;
        return thermometerId == that.thermometerId && temperature == that.temperature && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(thermometerId, timestamp, temperature);
    }
}