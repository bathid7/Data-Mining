import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import java.util.*;
import java.util.Arrays;
import java.lang.StringBuilder;
import java.util.function.BiConsumer;

public class KMeans{
	private static final Map<Integer,NetflixMovie> meanCenters = new HashMap<Integer,NetflixMovie>();
	// Canopy centers from step 2 is read and stored in meanCenters.
	private static final VoidFunction<String> READ_CENTRES =
      new VoidFunction<String>() {
        @Override
        public void call(String s) throws Exception {
          String[] data = s.split(",");
					int id = Integer.parseInt(data[0].substring(1,data[0].length()));
					NetflixMovie nm = new NetflixMovie(id, data[1].substring(0,data[1].length()-1));
					meanCenters.put(id,nm);
        }
  }; 
	// Each movie is mapped to a k-center
	// input: String:movie_id:ratingVector
	// output: Tuple<K-center,NetflixMovie object of the movie>
	private static final PairFunction<String, Integer, NetflixMovie> KCENTER_MAPPER =
      new PairFunction<String, Integer, NetflixMovie>() {
        @Override
        public Tuple2<Integer, NetflixMovie> call(String s) throws Exception {
          String[] data = s.split(",");
					String temp = "";
					if(data.length==3)
						temp+=data[1].substring(1,data[1].length()-1);
					else
						temp+=data[1].substring(1,data[1].length()-2);
					int id = Integer.parseInt(data[0].substring(1,data[0].length()));
					NetflixMovie nm = new NetflixMovie(id,temp);
					int center = assignCenters(nm);
          return new Tuple2<Integer, NetflixMovie>(center, nm);
        }
  }; 
	// Movie is assigned to k-center with maximum similarity.
	public static final Integer assignCenters(NetflixMovie temp){
			NetflixMovie keyRes = null;
			double maxDist = 0.0;
			for(NetflixMovie m2:meanCenters.values()){
				double t = temp.ComplexDistance(m2);
				if( t > maxDist){
					maxDist = t;
					keyRes = m2;
				}
			}
		return (Integer)keyRes.movie_id;
	}
	// Average vector for new center preparation.
	// Vector with all reviewers and their average ratings with in the cluster.
	public static final NetflixMovie avgRatings(List<NetflixMovie> temp){
		final Map<Integer,Double> mapRatings = new HashMap<Integer,Double>();
		final Map<Integer,Integer> mapCount = new HashMap<Integer,Integer>();
		temp.forEach(t->{
			t.features.forEach((k,v) ->{
				mapRatings.putIfAbsent(k,0.0);
				mapRatings.computeIfPresent(k, (a,b)-> b + v);
				mapCount.putIfAbsent(k,0);
				mapCount.computeIfPresent(k, (a,b)-> b + 1);
			});
		});
		mapRatings.replaceAll((k,v)-> (double)v/mapCount.get(k));
		return new NetflixMovie(0,mapRatings);
	}
	// List of netflixmovie objects in each cluster is changed to string of movieid concatinated by ':' 
	private static final Function<List<NetflixMovie>, String> REDUCE_TO_VALUES =
		new Function<List<NetflixMovie>, String>() {
			@Override
			public String call(List<NetflixMovie> rs) {
				StringBuilder builder = new StringBuilder();
				for(NetflixMovie t:rs){	
						if(builder.length()>0)
							builder.append(":");
						else
							builder.append("{");
						builder.append(t.movie_id);
				}
				builder.append("}");
				return builder.toString();
			}
	};
	// Iterable Netflixmovie is converted to list NetflixMovie for future processing.
	private static final Function<Iterable<NetflixMovie>, List<NetflixMovie>> ITER_TO_LIST =
		new Function<Iterable<NetflixMovie>, List<NetflixMovie>>() {
			@Override
			public List<NetflixMovie> call(Iterable<NetflixMovie> rs) {
				List<NetflixMovie> temp = new ArrayList<NetflixMovie>();
				for(NetflixMovie t:rs){
					temp.add(t);
				}
				return temp;
			}
	};
	
	public static void main(String[] args) {
    if (args.length < 4) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }
		int max_steps = Integer.parseInt(args[0]);
    SparkConf conf = new SparkConf().setAppName("org.sparkexample.KMeans").setMaster("local");
    JavaSparkContext context = new JavaSparkContext(conf);
    JavaRDD<String> centers = context.textFile(args[1]);
	  centers.foreach(READ_CENTRES);
		JavaRDD<String> data = context.textFile(args[2]);
		JavaPairRDD<Integer,List<NetflixMovie>> tempClusters;
		int steps = 1;
		do{
			JavaPairRDD<Integer, NetflixMovie> pairs = data.mapToPair(KCENTER_MAPPER);
			tempClusters = pairs.groupByKey().mapValues(ITER_TO_LIST);
			final Map<Integer, NetflixMovie> tempCenters = new HashMap<Integer,NetflixMovie>();
			BiConsumer<Integer, List<NetflixMovie>> assignNewCenters = (k, v) -> {
				NetflixMovie avgMovie = avgRatings(v);
				NetflixMovie keyRes = null;
				double maxVal = 0.0;
				for(NetflixMovie temp:v){
				  double tempDiff = temp.ComplexDistance(avgMovie);
					if(tempDiff > maxVal){
						maxVal = tempDiff;
						keyRes = temp;
					}
				}
				tempCenters.put(keyRes.movie_id,keyRes);
			};
			tempClusters.collectAsMap().forEach(assignNewCenters);
			meanCenters.clear();
			meanCenters.putAll(tempCenters);
			steps++;
		}while(steps<=max_steps);
		JavaPairRDD<Integer, String> result = tempClusters.mapValues(REDUCE_TO_VALUES);
		result.saveAsTextFile(args[3]);
  }
}