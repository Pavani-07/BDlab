import java.util.*;

public class AprioriHash {
    private static final int MIN_SUPPORT = 2;

    public static void main(String[] args) {
        // Sample transactions stored in a HashMap
        HashMap<Integer, Set<String>> transactions = new HashMap<>();
        transactions.put(1, Set.of("I1", "I2", "I5"));
        transactions.put(2, Set.of("I2", "I4"));
        transactions.put(3, Set.of("I2", "I3"));
        transactions.put(4, Set.of("I1", "I2", "I4"));
        transactions.put(5, Set.of("I1", "I3"));
        transactions.put(6, Set.of("I2", "I3"));
        transactions.put(7, Set.of("I1", "I3"));
        transactions.put(8, Set.of("I1", "I2", "I3", "I5"));
        transactions.put(9, Set.of("I1", "I2", "I3"));

        Map<Set<String>, Integer> frequentItemsets = findFrequentItemsets(transactions);
        System.out.println("Frequent Itemsets: " + frequentItemsets);
    }

    private static Map<Set<String>, Integer> findFrequentItemsets(HashMap<Integer, Set<String>> transactions) {
        Map<Set<String>, Integer> frequentItemsets = new HashMap<>();

        // Step 1: Find frequent 1-itemsets
        Map<String, Integer> itemCounts = getItemCounts(transactions);
        Map<Set<String>, Integer> L1 = new HashMap<>();
        for (Map.Entry<String, Integer> entry : itemCounts.entrySet()) {
            if (entry.getValue() >= MIN_SUPPORT) {
                L1.put(Set.of(entry.getKey()), entry.getValue());
            }
        }
        frequentItemsets.putAll(L1);

        // Step 2: Generate and prune 2-itemsets using a hash table
        Map<Integer, Integer> hashTableH2 = createHashTable(transactions);

        // Prune candidates from C2 if bucket count < MIN_SUPPORT
        List<Set<String>> C2 = generateCandidate2Itemsets(L1.keySet());
        List<Set<String>> prunedC2 = new ArrayList<>();
        for (Set<String> candidate : C2) {
            int bucket = hashFunction(candidate);
            if (hashTableH2.getOrDefault(bucket, 0) >= MIN_SUPPORT) {
                prunedC2.add(candidate);
            }
        }

        // Count support for pruned 2-itemsets
        Map<Set<String>, Integer> L2 = new HashMap<>();
        for (Set<String> transaction : transactions.values()) {
            for (Set<String> candidate : prunedC2) {
                if (transaction.containsAll(candidate)) {
                    L2.put(candidate, L2.getOrDefault(candidate, 0) + 1);
                }
            }
        }

        // Add frequent 2-itemsets to the final list
        for (Map.Entry<Set<String>, Integer> entry : L2.entrySet()) {
            if (entry.getValue() >= MIN_SUPPORT) {
                frequentItemsets.put(entry.getKey(), entry.getValue());
            }
        }

        return frequentItemsets;
    }

    private static Map<String, Integer> getItemCounts(HashMap<Integer, Set<String>> transactions) {
        Map<String, Integer> itemCounts = new HashMap<>();
        for (Set<String> transaction : transactions.values()) {
            for (String item : transaction) {
                itemCounts.put(item, itemCounts.getOrDefault(item, 0) + 1);
            }
        }
        return itemCounts;
    }

    private static List<Set<String>> generateCandidate2Itemsets(Set<Set<String>> items) {
        List<String> itemList = new ArrayList<>();
        for (Set<String> itemSet : items) {
            itemList.addAll(itemSet); // Flatten into a list of single items
        }

        List<Set<String>> candidates = new ArrayList<>();
        for (int i = 0; i < itemList.size(); i++) {
            for (int j = i + 1; j < itemList.size(); j++) {
                Set<String> candidate = new HashSet<>(Arrays.asList(itemList.get(i), itemList.get(j)));
                candidates.add(candidate);
            }
        }
        return candidates;
    }

    private static Map<Integer, Integer> createHashTable(HashMap<Integer, Set<String>> transactions) {
        Map<Integer, Integer> hashTable = new HashMap<>();
        for (Set<String> transaction : transactions.values()) {
            List<String> items = new ArrayList<>(transaction);
            for (int i = 0; i < items.size(); i++) {
                for (int j = i + 1; j < items.size(); j++) {
                    Set<String> pair = new HashSet<>(Arrays.asList(items.get(i), items.get(j)));
                    int bucket = hashFunction(pair);
                    hashTable.put(bucket, hashTable.getOrDefault(bucket, 0) + 1);
                }
            }
        }
        return hashTable;
    }

    private static int hashFunction(Set<String> itemset) {
        List<String> items = new ArrayList<>(itemset);
        Collections.sort(items);
        int orderX = items.get(0).charAt(1) - '0';
        int orderY = items.get(1).charAt(1) - '0';
        return ((orderX * 10) + orderY) % 7;
    }
}
