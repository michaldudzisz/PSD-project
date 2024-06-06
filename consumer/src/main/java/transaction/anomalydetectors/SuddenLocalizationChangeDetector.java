package transaction.anomalydetectors;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.Collector;
import transaction.dto.Fraud;
import transaction.dto.Transaction;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingInt;
import static transaction.anomalydetectors.SuddenLocalizationChangeDetector.LocalizationCentre;

public class SuddenLocalizationChangeDetector implements AggregateFunction<Transaction, Map<LocalizationCentre, List<Transaction>>, List<Fraud>> {

    private static final long maximalCentreDistance = 100; // km
    private static final long minimalAirplaneDistance = 300; // km
    private static final long maxCarVelocity = 150; // 150 km/h
    private static final long maxAirplaneVelocity = 900; // 150 km/h


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
//            System.out.println("distanceFromCentre: " + distanceFromCentre);
            // if the transaction can belong to localization group, add it:
            if (distanceFromCentre < maximalCentreDistance) {
//                System.out.println("Adding");
                // update localization centre as a mean of localizations in group:
                List<Transaction> transactionsWithinCentre = accumulator.get(centre);
                int groupSize = transactionsWithinCentre.size();
                double updatedLatitude = (centre.latitude * groupSize + latitude) / (groupSize + 1);
                double updatedLongitude = (centre.longitude * groupSize + longitude) / (groupSize + 1);
                LocalizationCentre updatedCentre = new LocalizationCentre(updatedLatitude, updatedLongitude);

//                System.out.println("In Adding: previousCentre: " + centre);
//                System.out.println("In Adding: newCentre: " + updatedCentre);

                // update accumulator, add transaction to updated centre:
                transactionsWithinCentre.add(transaction);
                accumulator.remove(centre);
                accumulator.put(updatedCentre, transactionsWithinCentre);

                added = true;
                break;
            }
        }

        // if there is not corresponding group, create a new one:
        if (!added) {
//            System.out.println("Not adding");
            ArrayList<Transaction> trs = new ArrayList<Transaction>() {{
                add(transaction);
            }};
            LocalizationCentre newLocalizationCentre = new LocalizationCentre(latitude, longitude);
            accumulator.put(newLocalizationCentre, trs);
//            System.out.println("newLocalizationCentre: " + newLocalizationCentre);
        }

        return accumulator;
    }

    @Override
    public List<Fraud> getResult(Map<LocalizationCentre, List<Transaction>> accumulator) {
        if (accumulator.isEmpty())
            return Collections.emptyList();

        // Find group with most elements and consider it a base group for user:
        Map.Entry<LocalizationCentre, List<Transaction>> mainGroup = accumulator.entrySet().stream()
                .max(Comparator.comparingInt(entry -> entry.getValue().size()))
                .orElse(null);

//        System.out.println("mainGroup.getValue().size(): " + mainGroup.getValue().size());
        // remove it from accumulator, save any other transactions as potential frauds:
        accumulator.remove(mainGroup.getKey());
        List<Transaction> potentialFrauds = accumulator
                .values().stream()
                .reduce((first, second) -> {
                    first.addAll(second);
                    return first;
                })
                .orElse(null);

        if (potentialFrauds == null)
            return Collections.emptyList();

        // for every potential fraud, check if there is a transaction from main group that would require too much velocity:
        List<Transaction> mainGroupTransactionsSortedByTime = mainGroup.getValue().stream()
                .sorted(Comparator.comparing(Transaction::getTimestamp))
                .collect(Collectors.toList());

        mainGroup.setValue(mainGroupTransactionsSortedByTime);

//        System.out.println("mainGroupTransactionsSortedByTime.size(): " + mainGroupTransactionsSortedByTime.size());
//        System.out.println("potentialFrauds.size(): " + potentialFrauds.size());

        List<Fraud> frauds = new ArrayList<>();
        for (Transaction potentialFraud : potentialFrauds) {
            // Find the closest transaction from main group in sense of timestamp:
            Transaction closestInTimeProperTransaction = mainGroupTransactionsSortedByTime.stream()
                    .sorted(Comparator.comparingLong(o -> Math.abs(o.getTimestamp().until(potentialFraud.getTimestamp(), java.time.temporal.ChronoUnit.SECONDS))))
                    .limit(1)
                    .collect(Collectors.toList()).get(0);

            double distance = DistanceCalculator.calculateDistance( // in km
                    potentialFraud.localization.latitude,
                    potentialFraud.localization.longitude,
                    closestInTimeProperTransaction.localization.latitude,
                    closestInTimeProperTransaction.localization.longitude
            );

            // if the distance is small enough, continue. There may be localization errors in small distances.
//            System.out.println("distance: " + distance);
//            if (distance < maximalCentreDistance)
//                continue;

            LocalDateTime properTime = closestInTimeProperTransaction.getTimestamp();
            LocalDateTime potentialFraudTime = potentialFraud.getTimestamp();
            Duration timeBetween;
            if (properTime.isBefore(potentialFraudTime)) {
                timeBetween = Duration.between(properTime, potentialFraudTime);
            } else {
                timeBetween = Duration.between(potentialFraudTime, properTime);
            }

            double timeBetweenHours = ((double) timeBetween.toMillis()) / (1000 * 60 * 60);

            boolean isFraud = false;
            double realVelocity = distance / timeBetweenHours;
            if (distance < minimalAirplaneDistance) {
                if (realVelocity > maxCarVelocity) isFraud = true;
            } else {
                if (realVelocity > maxAirplaneVelocity) isFraud = true;
            }

            if (isFraud) {
                frauds.add(new Fraud(potentialFraud,
                        "Localization changed too fast. Time between is " +
                                String.format("%.2f", timeBetweenHours) +
                                "hours. Computed velocity is: " +
                                String.format("%.2f", realVelocity) +
                                " for distance " +
                                String.format("%.2f", distance) +
                                ". One transaction timestamp is " +
                                properTime +
                                " the second is " +
                                potentialFraudTime
                ));
            }
        }

        return frauds;
    }

    @Override
    public Map<LocalizationCentre, List<Transaction>> merge(
            Map<LocalizationCentre, List<Transaction>> a,
            Map<LocalizationCentre, List<Transaction>> b
    ) {
        a.putAll(b);
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

        @Override
        public String toString() {
            return "LocalizationCentre{" +
                    "latitude=" + latitude +
                    ", longitude=" + longitude +
                    '}';
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
