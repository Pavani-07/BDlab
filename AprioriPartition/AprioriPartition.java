import java.util.*;

public class AprioriPartition {

    public static void main(String[] args) {
        Map<String, List<String>> transactions = new HashMap<>();
        transactions.put("T1", Arrays.asList("i1", "i2", "i5"));
        transactions.put("T2", Arrays.asList("i2", "i4"));
        transactions.put("T3", Arrays.asList("i2", "i3"));
        transactions.put("T4", Arrays.asList("i1", "i2", "i4"));
        transactions.put("T5", Arrays.asList("i1", "i3"));
        transactions.put("T6", Arrays.asList("i2", "i3"));
        transactions.put("T7", Arrays.asList("i1", "i3"));
        transactions.put("T8", Arrays.asList("i1", "i2", "i3", "i5"));
        transactions.put("T9", Arrays.asList("i1", "i2", "i3"));

        double minSupport = 0.2;  

        List<Map<String, List<String>>> partitions = partitionData(transactions, 3);

        Set<Set<String>> globalCandidates = new HashSet<>();
        for (Map<String, List<String>> partition : partitions) {
            Set<Set<String>> localFrequentItemsets = FreqItemsetsInPartition(partition, minSupport);
            globalCandidates.addAll(localFrequentItemsets);
        }

        Set<Set<String>> globalFrequentItemsets = countGlobalItemsets(globalCandidates, transactions, minSupport);

        System.out.println(globalFrequentItemsets);
    }

    
    
    public static Set<Set<String>> FreqItemsetsInPartition(Map<String,List<String>> partition, double minSupport){
    	Map<Set<String>, Integer> itemCounts = new HashMap<>();
    	Set<Set<String>> Itemsets = new HashSet<>();
    	for (List<String> transaction : partition.values()) {
            Set<String> transactionSet = new HashSet<>(transaction);
            List<Set<String>> subsets = generateSubsets(transactionSet);

            for (Set<String> subset : subsets) {
                itemCounts.put(subset, itemCounts.getOrDefault(subset, 0) + 1);
            }
        }
    	for (Map.Entry<Set<String>, Integer> entry : itemCounts.entrySet()) {
            if (entry.getValue() >= minSupport) {
            	Itemsets.add(entry.getKey());
            }
        }
    	return Itemsets;
     }
    
    
    public static List<Map<String, List<String>>> partitionData(Map<String, List<String>> transactions, int numPartitions) {
        List<Map<String, List<String>>> partitions = new ArrayList<>();
        int partitionSize = (int) Math.ceil((double) transactions.size() / numPartitions);
        List<String> transactionKeys = new ArrayList<>(transactions.keySet());

        for (int i = 0; i < numPartitions; i++) {
            Map<String, List<String>> partition = new HashMap<>();
            for (int j = i * partitionSize; j < (i + 1) * partitionSize && j < transactionKeys.size(); j++) {
                partition.put(transactionKeys.get(j), transactions.get(transactionKeys.get(j)));
            }
            partitions.add(partition);
        }

        return partitions;
    }
    
    public static Set<Set<String>> countGlobalItemsets(Set<Set<String>> candidates, Map<String, List<String>> transactions, double minSupport) {
        Map<Set<String>, Integer> globalItemCounts = new HashMap<>();
        int minSupportCount = (int) Math.ceil(minSupport * transactions.size());
        for (List<String> transaction : transactions.values()) {
            Set<String> transactionSet = new HashSet<>(transaction);
            for (Set<String> candidate : candidates) {
                if (transactionSet.containsAll(candidate)) {
                    globalItemCounts.put(candidate, globalItemCounts.getOrDefault(candidate, 0) + 1);
                }
            }
        }
        Set<Set<String>> globalFrequentItemsets = new HashSet<>();
        for (Map.Entry<Set<String>, Integer> entry : globalItemCounts.entrySet()) {
            if (entry.getValue() >= minSupportCount) {
                globalFrequentItemsets.add(entry.getKey());
            }
        }

        return globalFrequentItemsets;
    }
    
    public static List<Set<String>> generateSubsets(Set<String> set) {
        List<Set<String>> subsets = new ArrayList<>();
        List<String> list = new ArrayList<>(set);
        int n = list.size();
        subsets.add(new HashSet<>());
        for (int i = 0; i < n; i++) {
            String element = list.get(i);
            int currentSubsetSize = subsets.size();
            for (int j = 0; j < currentSubsetSize; j++) {
                Set<String> newSubset = new HashSet<>(subsets.get(j));
                newSubset.add(element);
                subsets.add(newSubset);
            }
        }
        return subsets;
    }

}
