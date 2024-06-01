package temperature;

import java.time.LocalDateTime;

public class Alarm {

    public LocalDateTime atTime;

    public float temperature;

    public Alarm() {}

    public Alarm(LocalDateTime atTime, float temperature) {
        this.atTime = atTime;
        this.temperature = temperature;
    }

    public static Alarm fromMeasurement(Transaction transaction) {
        return new Alarm(transaction.timestamp, transaction.temperature);
    }

    public LocalDateTime getAtTime() {
        return atTime;
    }

    public void setAtTime(LocalDateTime atTime) {
        this.atTime = atTime;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }
}
