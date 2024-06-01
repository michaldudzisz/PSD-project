package temperature;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Objects;


public class Transaction {

    public Integer cardId;

    public LocalDateTime timestamp;

    public BigDecimal value;

    public Integer userId;

    public Integer limit;

    public Localization localization;

    public Transaction() {
    }

    public Transaction(Integer cardId, LocalDateTime timestamp, BigDecimal value, Integer userId, Integer limit, Localization localization) {
        this.cardId = cardId;
        this.timestamp = timestamp;
        this.value = value;
        this.userId = userId;
        this.limit = limit;
        this.localization = localization;
    }

    public boolean isAboveLimit() {
        int comparisonResult = value.compareTo(BigDecimal.valueOf(limit));
        return comparisonResult > 0;
    }

    public Integer getCardId() {
        return cardId;
    }

    public void setCardId(Integer cardId) {
        this.cardId = cardId;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public Localization getLocalization() {
        return localization;
    }

    public void setLocalization(Localization localization) {
        this.localization = localization;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Objects.equals(cardId, that.cardId)
                && Objects.equals(timestamp, that.timestamp)
                && Objects.equals(value, that.value)
                && Objects.equals(userId, that.userId)
                && Objects.equals(limit, that.limit)
                && Objects.equals(localization, that.localization);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cardId, timestamp, value, userId, limit, localization);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "cardId=" + cardId +
                ", timestamp=" + timestamp +
                ", value=" + value +
                ", userId=" + userId +
                ", limit=" + limit +
                ", localization=" + localization +
                '}';
    }

    public static class Localization {
        public Double latitude;
        public Double longitude;

        public Localization() {}

        public Localization(Double latitude, Double longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public Double getLatitude() {
            return latitude;
        }

        public void setLatitude(Double latitude) {
            this.latitude = latitude;
        }

        public Double getLongitude() {
            return longitude;
        }

        public void setLongitude(Double longitude) {
            this.longitude = longitude;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Localization that = (Localization) o;
            return Objects.equals(latitude, that.latitude) && Objects.equals(longitude, that.longitude);
        }

        @Override
        public int hashCode() {
            return Objects.hash(latitude, longitude);
        }
    }
}