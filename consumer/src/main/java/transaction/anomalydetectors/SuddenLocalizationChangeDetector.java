package transaction.anomalydetectors;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.Collector;
import transaction.dto.Fraud;
import transaction.dto.Transaction;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingInt;
import static transaction.anomalydetectors.SuddenLocalizationChangeDetector.LocalizationCentre;

public class SuddenLocalizationChangeDetector implements AggregateFunction<Transaction, Map<LocalizationCentre, List<Transaction>>, List<Fraud>> {

    private static final BigDecimal minimalValue = BigDecimal.valueOf(1.0);
    private static final long maximalCentreDistance = 100; // km

    private static final Duration windowLength = Duration.ofDays(1);
    private static final Duration windowSlide = Duration.ofDays(1);

    public static DataStream<Fraud> suddenLocalizationChangeDetector(DataStream<Transaction> dataStream) {
        return dataStream.keyBy(Transaction::getUserId)
                .window(SlidingEventTimeWindows.of(windowLength, windowSlide))
                .aggregate(new SuddenLocalizationChangeDetector())
                .flatMap((List<Fraud> frauds, Collector<Fraud> out) -> frauds.forEach(out::collect))
                .returns(TypeInformation.of(Fraud.class));
    }

    @Override
    public Map<LocalizationCentre, List<Transaction>> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<LocalizationCentre, List<Transaction>> add(
            Transaction transaction,
            Map<LocalizationCentre, List<Transaction>> accumulator
    ) {
        double latitude = transaction.localization.getLatitude();
        double longitude = transaction.localization.getLongitude();

        // check if there is already localization group that this transaction can belong to:
        boolean added = false;
        for (LocalizationCentre centre : accumulator.keySet()) {
            double distanceFromCentre = DistanceCalculator.calculateDistance(centre.latitude, centre.longitude, latitude, longitude);
            if (distanceFromCentre < maximalCentreDistance) {
                accumulator.get(centre).add(transaction);
                added = true;
                break;
            }
        }

        // if there is not corresponding group, create a new one:
        if (!added) {
            ArrayList<Transaction> trs = new ArrayList<Transaction>(){{ add(transaction); }};
            accumulator.put(new LocalizationCentre(latitude, longitude), trs);
        }

        return accumulator;
    }

    @Override
    public List<Fraud> getResult(Map<LocalizationCentre, List<Transaction>> accumulator) {
        // Find group with most elements and consider it a base group:
        Map.Entry<LocalizationCentre, List<Transaction>> biggestGroup = accumulator.entrySet().stream()
                .sorted(comparingInt(entry -> entry.getValue().size()))
                .reduce((first, second) -> second)
                .orElse(null);

        biggestGroup

        if (accumulator.isEmpty())
            return Collections.emptyList();

        List<Transaction> sortedTransactions = accumulator.stream()
                .sorted(comparing(Transaction::getValue))
                .collect(Collectors.toList());

        int p05Index = (int) (sortedTransactions.size() * 0.05);
        Transaction p05Transaction = sortedTransactions.get(p05Index);

        if (p05Transaction.getValue().compareTo(minimalValue) < 0) {
            return accumulator.stream()
                    .filter(transaction -> transaction.getValue().compareTo(minimalValue) < 0)
                    .map(transaction -> new Fraud(transaction, "5th percentile lower than " + minimalValue))
                    .collect(Collectors.toList());
        } else
            return Collections.emptyList();
    }

    @Override
    public Map<LocalizationCentre, List<Transaction>> merge(
            Map<LocalizationCentre, List<Transaction>> a,
            Map<LocalizationCentre, List<Transaction>> b
    ) {
        a.addAll(b);
        return a;
    }

    public static class LocalizationCentre {
        public double latitude;
        public double longitude;

        public LocalizationCentre(double latitude, double longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LocalizationCentre that = (LocalizationCentre) o;
            return Double.compare(latitude, that.latitude) == 0 && Double.compare(longitude, that.longitude) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(latitude, longitude);
        }
    }

    public static class DistanceCalculator {
        private static final double EARTH_RADIUS_KM = 6371.0;
        public static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);
            double a = Math.pow(Math.sin(dLat / 2), 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.pow(Math.sin(dLon / 2), 2);
            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return EARTH_RADIUS_KM * c;
        }
    }
}
