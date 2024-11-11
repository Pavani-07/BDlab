import java.util.*;

public class AprioriOne {
    public static void main(String[] args) {
        Map<String, List<String>> transactions = new HashMap<>();
        transactions.put("T100", Arrays.asList("I1", "I2", "I5"));
        transactions.put("T200", Arrays.asList("I2", "I4"));
        transactions.put("T300", Arrays.asList("I2", "I3"));
        transactions.put("T400", Arrays.asList("I1", "I2", "I4"));
        transactions.put("T500", Arrays.asList("I1", "I3"));
        transactions.put("T600", Arrays.asList("I2", "I3"));
        transactions.put("T700", Arrays.asList("I1", "I3"));
        transactions.put("T800", Arrays.asList("I1", "I2", "I3", "I5"));
        transactions.put("T900", Arrays.asList("I1", "I2", "I3"));

        int minSupport = 2;
        List<List<String>> frequentItemsets = apriori(transactions, minSupport);

        System.out.println("Frequent itemsets: " + frequentItemsets);
    }

    public static List<List<String>> apriori(Map<String, List<String>> transactions, int minSupport) {
        List<List<String>> frequentItemsets = new ArrayList<>();
        Map<List<String>, Integer> itemCounts = new HashMap<>();

        Map<String, Integer> counts = getItemCounts(transactions);
        List<List<String>> L1 = getFrequentItems(counts, minSupport);
        frequentItemsets.addAll(L1);

        List<List<String>> Lk = L1;
        int k = 2;
        while (!Lk.isEmpty()) {
            List<List<String>> Ck = aprioriGen(Lk, k);

            itemCounts = countCandidates(Ck, transactions);

            Lk = getFrequentItemsList(itemCounts, minSupport);
            frequentItemsets.addAll(Lk);
            k++;
        }
        return frequentItemsets;
    }

    public static Map<String, Integer> getItemCounts(Map<String, List<String>> transactions) {
        Map<String, Integer> counts = new HashMap<>();
        for (List<String> items : transactions.values()) {
            for (String item : items) {
                counts.put(item, counts.getOrDefault(item, 0) + 1);
            }
        }
        return counts;
    }

    public static List<List<String>> getFrequentItems(Map<String, Integer> counts, int minSupport) {
        List<List<String>> frequentItems = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            if (entry.getValue() >= minSupport) {
                frequentItems.add(Collections.singletonList(entry.getKey()));
            }
        }
        return frequentItems;
    }

    public static List<List<String>> aprioriGen(List<List<String>> Lk, int k) {
        List<List<String>> candidates = new ArrayList<>();
        for (int i = 0; i < Lk.size(); i++) {
            for (int j = i + 1; j < Lk.size(); j++) {
                List<String> l1 = new ArrayList<>(Lk.get(i));
                List<String> l2 = new ArrayList<>(Lk.get(j));
                Collections.sort(l1);
                Collections.sort(l2);
                
                if (canJoin(l1, l2, k - 1)) {
                    List<String> candidate = new ArrayList<>(l1);
                    candidate.add(l2.get(k - 2));
                    Collections.sort(candidate);

                    if (!hasInfrequentSubset(candidate, Lk)) {
                        candidates.add(candidate);
                    }
                }
            }
        }
        return candidates;
    }

    public static boolean canJoin(List<String> l1, List<String> l2, int length) {
        for (int i = 0; i < length - 1; i++) {
            if (!l1.get(i).equals(l2.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static boolean hasInfrequentSubset(List<String> candidate, List<List<String>> Lk) {
        for (int i = 0; i < candidate.size(); i++) {
            List<String> subset = new ArrayList<>(candidate);
            subset.remove(i);
            if (!Lk.contains(subset)) {
                return true;
            }
        }
        return false;
    }

    public static Map<List<String>, Integer> countCandidates(List<List<String>> candidates, Map<String, List<String>> transactions) {
        Map<List<String>, Integer> counts = new HashMap<>();
        for (List<String> candidate : candidates) {
            int count = 0;
            for (List<String> transaction : transactions.values()) {
                if (transaction.containsAll(candidate)) {
                    count++;
                }
            }
            counts.put(candidate, count);
        }
        return counts;
    }

    public static List<List<String>> getFrequentItemsList(Map<List<String>, Integer> itemCounts, int minSupport) {
        List<List<String>> frequentItems = new ArrayList<>();
        for (Map.Entry<List<String>, Integer> entry : itemCounts.entrySet()) {
            if (entry.getValue() >= minSupport) {
                frequentItems.add(entry.getKey());
            }
        }
        return frequentItems;
    }
}
