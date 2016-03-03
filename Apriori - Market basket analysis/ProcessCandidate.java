import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.*;
import java.util.stream.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

// Structure for storing candidates.
class Node2{
   private List<Integer> children; 
   public Node2(){
   this.children = new ArrayList<>();
   } 
   public List<Integer> returnChild(){
     return this.children;
   }
}

public class ProcessCandidate{
    
	private static List<List<Integer>> transactions = new ArrayList<>();
	private static Map<Set<Integer>,Integer> itemset = Collections.synchronizedMap(new ConcurrentHashMap<>());
	private static Map<Integer, Node2> tree2 = Collections.synchronizedMap(new ConcurrentHashMap<>());
	
	// Parallel creation of candidates of level 2
   public void createLevel2(List<Set<Integer>> temp){
        try {
		    final List<Callable<Map<Integer,Node2>>> partitions = new ArrayList<Callable<Map<Integer,Node2>>>(); 
        int num = 4;
        int size = temp.size();
        int nPartision = size/num;
        if(nPartision == 0 ){ nPartision=1; num = size;}
				for(int i = 0; i < num; i++) {
				final int lower = (i * nPartision);
				final int upper = (i == num-1)? temp.size():lower + nPartision;
				partitions.add(new Callable<Map<Integer,Node2>>() {
				public Map<Integer,Node2> call() { 
                        return createCandidates(temp, lower, upper);
											}        
									});
				}
        final ExecutorService executorPool = 
        Executors.newFixedThreadPool(6); 
        final List<Future<Map<Integer,Node2>>> result =
        executorPool.invokeAll(partitions);
        executorPool.shutdown(); 
        for(Future<Map<Integer,Node2>> t : result){
						tree2.putAll(t.get());
          }
        }
       catch(Exception ex) { throw new RuntimeException(ex); }
        System.gc();
		}
		
		// Individual tasks for storing candidates in the tree.
		public Map<Integer, Node2> createCandidates(List<Set<Integer>> temp, int lower, int upper){
			System.gc();
			Map<Integer, Node2> temp2 = new HashMap<>();
			IntStream.range(lower, upper)
             .forEach(i -> {
                int a = temp.get(i).stream().collect(Collectors.toList()).get(0);
								Node2 n2 = new Node2();
								temp2.put(a,n2);
                IntStream.range(i+1, temp.size())
								         .forEach(j -> {
                            int b = temp.get(j).stream().collect(Collectors.toList()).get(0);
										        n2.returnChild().add(b);									
                });                         
      });   
			System.gc();
			return temp2;
		}
		
    // Candidates of size K.	
    public void createCandidates(List<Set<Integer>> temp, int length){
      System.gc();
      IntStream.range(0, temp.size())
               .forEach(i -> {
                    List<Integer> t1 = Arrays.asList(temp.get(i).toArray(new Integer[0]));
										Collections.sort(t1);
                    IntStream.range(i+1, temp.size())
                             .forEach(j -> {
                                List<Integer> t2 = Arrays.asList(temp.get(j).toArray(new Integer[0]));
																Collections.sort(t2);
                                Set<Integer> t3 = new HashSet<>();
                                t3.addAll(t1.subList(length, t1.size()));
                                t3.addAll(t2.subList(length, t2.size()));
                                if(t2.subList(0,length).equals(t1.subList(0,length))){
                                    t3.clear();
                                    t3.addAll(temp.get(i));
                                    t3.addAll(temp.get(j));
																		itemset.putIfAbsent(t3.stream().sorted().collect(Collectors.toSet()), 0);                                                               
                              }                         
                        });
								});
			System.gc();
    }
		
		// Calculate the frequency of occurrence of each candidate in the transaction.
	  public Map<Set<Integer>,Integer> calcFrequency(List<List<Integer>> transaction, int length) throws InterruptedException, IOException, Exception{
        this.transactions = transaction;
        Map<Set<Integer>,Integer> temp3= new ConcurrentHashMap<>();
        try {
		    final List<Callable<Map<Set<Integer>,Integer>>> partitions = new ArrayList<Callable<Map<Set<Integer>,Integer>>>(); 
        int num = 4;
        int size = transactions.size();
        int nPartision = size/num;
        if(nPartision == 0 ){ nPartision=1; num = size;}
				for(int i = 0; i < num; i++) {
				final int lower = (i * nPartision);
				final int upper = (i == num-1)? transactions.size():lower + nPartision;
				partitions.add(new Callable<Map<Set<Integer>,Integer>>() {
				public Map<Set<Integer>,Integer> call() { 
                        return prallelTasks(lower, upper, length);
											}        
									});
				}
        final ExecutorService executorPool = 
        Executors.newFixedThreadPool(6); 
        final List<Future<Map<Set<Integer>,Integer>>> result =
        executorPool.invokeAll(partitions);
        executorPool.shutdown(); 
        for(Future<Map<Set<Integer>,Integer>> t : result){
          t.get().forEach((k,v) -> {
              if(temp3.containsKey(k))
                temp3.compute(k, (a,b) -> b+v);
              else
                temp3.put(k,v);
            });
          }
        }
       catch(Exception ex) { throw new RuntimeException(ex); }
        itemset.clear();			 
        tree2.clear();
        System.gc();
        return temp3;
    }
		
		// Parallel Tasks
    public Map<Set<Integer>,Integer> prallelTasks(int lower, int upper, int length){  
    Map<Set<Integer>,Integer> temp2 = new ConcurrentHashMap<>();    
    transactions.subList(lower, upper)
              .stream()
              .forEach(t -> {
							          Collections.sort(t);
                        List<Set<Integer>> temp;
												if(length == 2 )
												  temp = process2(t);
												else
												  temp = processN(t);
                        if( temp != null){
                            temp.forEach(t1 -> {temp2.putIfAbsent(t1, 0);
                            temp2.computeIfPresent(t1, (a,b)->b+1);    
                        });                                                       
                    }     
               });
        return temp2;    
    }
		
    // Process of candidates of size 2.
		public List<Set<Integer>> process2(List<Integer> item){
        List<Set<Integer>> res = new ArrayList<>();
        for(int i=0; i<item.size();i++){
            int a = item.get(i);
            if(!tree2.containsKey(a))
                continue;
            Node2 n = tree2.get(a);
            IntStream.range(i+1, item.size())
                     .forEach(j -> {                                                
                        int b = item.get(j);
						            if(n.returnChild().contains(b))
														res.add(new HashSet<Integer>(Arrays.asList(a, b)));                                                        
						});                         
				} 
        return res;
    }
		
		// Process of candidates of size N.
    public List<Set<Integer>> processN(List<Integer> item){
      	List<Set<Integer>> res = new ArrayList<>();
				res = itemset.keySet().stream()
				            .filter(k -> item.containsAll(k))
										.collect(Collectors.toList());
        return res;
    }
}