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

public class CanopyCluster {
	private static final ArrayList<NetflixMovie> canopyCenters = new ArrayList<NetflixMovie>();
	private static final Set<Integer> centersIds = new HashSet<Integer>();
	// Canopies are read into memory.
	private static final VoidFunction<String> CANOPY_CENTERS =
      new VoidFunction<String>() {
        @Override
        public void call(String s) throws Exception {
          String[] data = s.split(",");
          NetflixMovie nm = new NetflixMovie(Integer.parseInt(data[0].substring(1,data[0].length())), data[1].substring(0,data[1].length()-1));
					centersIds.add(Integer.parseInt(data[0].substring(1,data[0].length())));
					canopyCenters.add(nm);
        }
  }; 
	// String movie is converted to tuple<movieid,rating vector>
	private static final PairFunction<String, Integer, String> WORDS_EXTRACTOR =
      new PairFunction<String, Integer, String>() {
        @Override
        public Tuple2<Integer, String> call(String s) throws Exception {
          String[] data = s.split(",");
					int id = Integer.parseInt(data[0].substring(1,data[0].length())); 
          return new Tuple2<Integer, String>(id, data[1].substring(0,data[1].length()-1));
        }
  }; 
	// For each movie, it is assigned to canopy based on the matching count.
  private static final Function<String,String> CANOPIES =
      new Function<String,String>() {
        @Override
        public String call(String s) throws Exception {
				  String res = "{"+s+"}";
          NetflixMovie m1 = new NetflixMovie(0,s);
          Boolean too_close = false;
					StringBuilder builder = new StringBuilder();
          for(NetflixMovie m2:canopyCenters){
            int matchCount = m1.MatchCount(m2,8);
            if(matchCount > 8){
								too_close |= true;
								 if(builder.length()>0)
                    builder.append(".");
								 else
									  builder.append("{Canopies:");	
                 builder.append(m2.movie_id);
						}
          }
          if(too_close){
						builder.append("}");
            String temp = builder.toString();
						res +=","+temp;
          }
					return res;         
        }
  };
	
	public static void main(String[] args) {
    if (args.length < 3) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }
    SparkConf conf = new SparkConf().setAppName("org.sparkexample.Canopy").setMaster("local");
    JavaSparkContext context = new JavaSparkContext(conf);
    JavaRDD<String> centers = context.textFile(args[0]);
	  centers.foreach(CANOPY_CENTERS);
		JavaRDD<String> data = context.textFile(args[1]);
	  JavaPairRDD<Integer, String> pairs = data.mapToPair(WORDS_EXTRACTOR);
    JavaPairRDD<Integer, String> result = pairs.mapValues(CANOPIES);
	  result.saveAsTextFile(args[2]);
  }
}