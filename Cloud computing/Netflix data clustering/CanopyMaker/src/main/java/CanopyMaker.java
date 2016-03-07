import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import java.util.*;
import java.util.Arrays;

public class CanopyMaker {
	private static final ArrayList<NetflixMovie> canopyCenters = new ArrayList<NetflixMovie>();
	// String is converted to Tuple<Movie id, rating vector>
	private static final PairFunction<String, Integer, String> WORDS_EXTRACTOR =
      new PairFunction<String, Integer, String>() {
        @Override
        public Tuple2<Integer, String> call(String s) throws Exception {
          String[] data = s.split(",");
          return new Tuple2<Integer, String>(Integer.parseInt(data[0].substring(1,data[0].length())), data[1].substring(0,data[1].length()-1));
        }
  }; 
	// Each tuple is checked whether it is canopy center or not based on match count.
  private static final Function<Tuple2<Integer,String>, Boolean> CANOPIES =
      new Function<Tuple2<Integer,String>, Boolean>() {
        @Override
        public Boolean call(Tuple2<Integer, String> s) throws Exception {
          NetflixMovie m1 = new NetflixMovie(s._1(),s._2());
          Boolean too_close = false;
          for(NetflixMovie m2:canopyCenters){
            int matchCount = m2.MatchCount(m1);
            if(matchCount > 8 || matchCount < 2)
              too_close = true;
          }
          if(!too_close){
            canopyCenters.add(m1);
          }

          return (!too_close);          
        }
  };

	public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }
    SparkConf conf = new SparkConf().setAppName("org.sparkexample.Canopy").setMaster("local");
    JavaSparkContext context = new JavaSparkContext(conf);
    JavaRDD<String> file = context.textFile(args[0]);
	  JavaPairRDD<Integer, String> pairs = file.mapToPair(WORDS_EXTRACTOR);
    JavaPairRDD<Integer, String> result  = pairs.filter(CANOPIES);
	  result.saveAsTextFile(args[1]);
  }
}