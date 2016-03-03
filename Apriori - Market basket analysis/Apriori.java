import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.function.*;
import java.util.stream.*;

public class Apriori{
   public static Map<Set<Integer>,Integer> itemset = Collections.synchronizedMap(new ConcurrentHashMap<>());
   public static List<List<Integer>> transactions = new ArrayList<>();
   public static Map<String,Integer> transList = new ConcurrentHashMap<>();
   public static int k;
   public static int s;
   public static String link;
   public static int uI = 1;
  /*
	   args[0] = s
	   args[1] = k
	   args[2] = file path
  */
   public static void main(String[] args)throws IOException, InterruptedException, Exception{
		s = Integer.parseInt(args[0]);
		k = Integer.parseInt(args[1]); 
		link = args[2];
		String outputFile = "out_s="+s+"_k="+k+"+.txt";
    BufferedWriter output = new BufferedWriter(new FileWriter(outputFile));
    Apriori a = new Apriori();
		ProcessCandidate c = new ProcessCandidate();
    long start = System.currentTimeMillis();
    a.readData(); 
    a.checkSupport(2);
		int i = 2;
		if(k==1)
			a.writeData(output);
    while(itemset.size()!=0){
      if(i == 2)
				c.createLevel2(new ArrayList<>(itemset.keySet()));
      else
        c.createCandidates(new ArrayList<>(itemset.keySet()), i-2);	
			itemset.clear();
			itemset = c.calcFrequency(transactions,i);		
			a.checkSupport(i+1);
			if(itemset.size()!=0 && i>=k){
				a.writeData(output);
			}
			i++;
    }
		output.close();
    System.out.println("\nTime Taken for mining : "+(System.currentTimeMillis() - start)/1000+" Seconds");
   }
	  
	 // Read data from the file. Convert the strings into numbers.
    public void readData()throws FileNotFoundException, IOException {
    BufferedReader data = new BufferedReader(new FileReader(link));
    List<List<String>> temp2 = new ArrayList<>();
    while (data.ready()) {   
      String line=data.readLine();
      List<String> temp3 = new ArrayList<>(Arrays.asList(line.split(" ")));
      temp2.add(temp3);
      temp3.forEach(p->{
         transList.computeIfAbsent(p, q->uI++);
      });
    }
		temp2 =temp2.stream()
								.filter(x -> x.size() >= k )
								.collect(Collectors.toList());
		System.out.println("  "+temp2.size());
    temp2.forEach(s -> {
      List<Integer> t = new ArrayList<>();
      s.forEach(q->{
        int id = transList.get(q);
        t.add(id);
				Set<Integer> temp = new HashSet<>();
				temp.add(id);
        itemset.putIfAbsent(temp, 0);
        itemset.computeIfPresent(temp, (a,b)->b+1);
      });
	    Collections.sort(t);
      transactions.add(t);
    });
		System.out.println("  "+itemset.size());
    pruneTransactionSize(2);	
		data.close();
  }
  
	// Write the output data into the file
	 public void writeData(BufferedWriter output)throws FileNotFoundException, IOException {
		itemset.forEach((k,v)->{
			k.forEach(j -> {
				transList.forEach((a1,b)-> { 
					if(j.equals(b)){
						try{     
							output.write(a1+" ");
						}
						catch(Exception ex){throw new RuntimeException();}
					}
				});
			});
			try{
				output.write("("+v+")");
				output.newLine();
			}
			catch(Exception ex){throw new RuntimeException();}
	  });
	}
	 
   // Remove the item set that has support less than S.
   public void checkSupport(int length){
    Set<Integer> temp = new HashSet<>();
    itemset.forEach((k,v)-> {
      if(v<s)
        itemset.remove(k);
      else
        temp.addAll(k);
		});
		pruneItemSet(temp, length);
   }
  
  // Retain items in the transactions that are frequent individually.	
  public void pruneItemSet(Set<Integer> temp, int length){
		for(int i=0; i<transactions.size();i++){
      transactions.get(i).retainAll(temp);
    } 
    pruneTransactionSize(length);							   
   }
   
	 // Filter out the transactions whose size is less than the current level.
   public void pruneTransactionSize(int length){
		transactions = transactions.stream()
                               .filter(x -> x.size() >= length)
                               .collect(Collectors.toList());
	}
}