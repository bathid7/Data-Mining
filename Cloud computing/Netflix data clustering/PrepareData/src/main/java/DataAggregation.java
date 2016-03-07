import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.Map;
import java.util.Arrays;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DataAggregation{
	// Mapper creates a pairRDD tuples<movieid,rating_vector> from the input movies.
  private static final PairFunction<String, Integer, String> WORDS_MAPPER =
      new PairFunction<String, Integer, String>() {
        @Override
        public Tuple2<Integer, String> call(String s) throws Exception {
          String[] data = s.split(",");
          return new Tuple2<Integer, String>(Integer.parseInt(data[0]), data[1]+":"+data[2]);
        }
  };
	// ALl the values of one key are reduced to a string concatinated by "|"
  private static final Function2<String, String, String> WORDS_REDUCER =
      new Function2<String, String, String>() {
        @Override
        public String call(String a, String b) throws Exception {
          return a+"|"+b;
        }
  };

	public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }
    SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local");
    JavaSparkContext context = new JavaSparkContext(conf);
    JavaRDD<String> file = context.textFile(args[0]);
    JavaPairRDD<Integer, String> pairs = file.mapToPair(WORDS_MAPPER);
    JavaPairRDD<Integer, String> counter = pairs.reduceByKey(WORDS_REDUCER);
    counter.coalesce(1,false).saveAsTextFile(args[1]);
  }
}
