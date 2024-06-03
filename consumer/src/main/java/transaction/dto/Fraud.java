package transaction.dto;

import java.util.Objects;

public class Fraud {

    public Transaction transaction;

    public String fraudReason;

    public Fraud() {}

    public Fraud(Transaction transaction, String fraudReason) {
        this.transaction = transaction;
        this.fraudReason = fraudReason;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public String getFraudReason() {
        return fraudReason;
    }

    public void setFraudReason(String fraudReason) {
        this.fraudReason = fraudReason;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Fraud fraud = (Fraud) o;
        return Objects.equals(transaction, fraud.transaction) && Objects.equals(fraudReason, fraud.fraudReason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transaction, fraudReason);
    }

    @Override
    public String toString() {
        return "Fraud{" +
                "transaction=" + transaction +
                ", fraudReason='" + fraudReason + '\'' +
                '}';
    }
}
