import java.util.*;
import java.util.Map.Entry;

public class AprioriDIC {

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
        
        int blockSize = 3;
        double minSupport = 0.2;
        
        List<Map<String,List<String>>> blocks = makePartition(transactions, blockSize);
        Set<Set<String>> freqItemsets = new HashSet<>();
        Map<Set<String>,Integer> globalCounts = new HashMap<>();
        
        for(Map<String,List<String>> block: blocks) {
        	updateFreqSets(block, freqItemsets, globalCounts, transactions.size(),minSupport);
        }
        
        Set<Set<String>> finalFreq = new HashSet<>();
        int minSupportCount = (int) Math.ceil(minSupport * transactions.size());
        for (Map.Entry<Set<String>, Integer> entry : globalCounts.entrySet()) {
            if (entry.getValue() >= minSupportCount) {
                finalFreq.add(entry.getKey());
            }
        }
        System.out.println(finalFreq);
	}

	public static List<Map<String,List<String>>> makePartition(Map<String, List<String>> transactions, int n){
		List<Map<String,List<String>>> blocks = new ArrayList<>();
		List<String> transKeys = new ArrayList<>(transactions.keySet());
		for(int i=0;i<n;i++) {
			Map<String,List<String>> block = new HashMap<>();
			for(int j=i*n;j<(i+1)*n && j<transKeys.size();j++) {
				block.put(transKeys.get(j), transactions.get(transKeys.get(j)));
			}
			blocks.add(block);
		}
		return blocks;
	}
	
	public static void updateFreqSets(Map<String,List<String>> block, Set<Set<String>> freqItemsets, Map<Set<String>,Integer> globalCounts, int size, double min) {
		int minS = (int) Math.ceil(size*min);
		Map<Set<String>,Integer> itemCounts = new HashMap<>();
		for(List<String> trans: block.values()) {
			Set<String> transactionSet = new HashSet<>(trans);
            List<Set<String>> subsets = generateSubsets(transactionSet);

            for (Set<String> subset : subsets) {
                itemCounts.put(subset, itemCounts.getOrDefault(subset, 0) + 1);
                globalCounts.put(subset, globalCounts.getOrDefault(subset, 0) + 1);
            }
		}
		for (Entry<Set<String>, Integer> entry : itemCounts.entrySet()) {
            if (entry.getValue() >= minS) {
                freqItemsets.add(entry.getKey());
            }
        }
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
