package spark.spark;

import spark.movie.MovieFromCSV;
import spark.movie.ParseMovie;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkFunctions {
    private static List<String> practice;
	
	private static JavaSparkContext sparkContext;
    
    JavaRDD<String> csvFile;

    // place inside init HTTP Servlet
    public static void initArrayPractice() {
		practice = new ArrayList<String>();
		practice.add("Avengers");
		practice.add("Avengers");
		practice.add("Star Wars");
		practice.add("Star Trek");
		practice.add("Star Trek");
		practice.add("Star Trek");
		practice.add("Star Trek");
		practice.add("Jurassic Park");
		practice.add("Jurassic Park");
		practice.add("Jurassic Park");
    }

    // inside init HTTP Servlet
    public static void initSpark(){
        SparkConf conf = new SparkConf().setAppName("project-1").setMaster("local");
		//conf.set("spark.testing.memory", "2147480000");
		sparkContext = new JavaSparkContext(conf);
    }

    public static List<Tuple2<String, Integer>> reduceByKeyArray(){
        initArrayPractice();
    	JavaRDD<String> namesRDD = sparkContext.parallelize(practice);
		JavaPairRDD<String, Integer> namesMapper = namesRDD.mapToPair((f) -> new Tuple2<>(f, 1));
		System.out.println(namesMapper.collect());
		JavaPairRDD<String, Integer> countNames = namesMapper.reduceByKey((x, y) -> ((int) x + (int) y));
        return countNames.collect();
    }

    public static List<Tuple2<String, Integer>> reduceByKeySimpleCSVTest() {
		JavaRDD<String> csvFile = sparkContext.textFile("movies_test.csv");
		JavaPairRDD<String, Integer> namesMapper = csvFile.mapToPair((f) -> new Tuple2<>(f, 1));
		System.out.println(namesMapper.collect());
		JavaPairRDD<String, Integer> countNames = namesMapper.reduceByKey((x, y) -> ((int) x + (int) y));
        return countNames.collect();
    }

    public static List<Tuple2<String, Integer>> reduceByKeyCSVMovie() {
		JavaRDD<String> allRows = sparkContext.textFile(new File("movies_test.csv").getAbsolutePath()).cache();
		//filter out headers
		String header = allRows.first();
		JavaRDD<String> headerlessCsvFile = allRows.filter(row -> !row.equals(header)); //.cache();
		// map to MyCSVFile data struct
        JavaRDD<MovieFromCSV> filteredHeaderlessRow = headerlessCsvFile.map((n) -> ParseMovie.parseCSV(n));

		JavaPairRDD<String, Integer> namesMapper = filteredHeaderlessRow.mapToPair((f) -> new Tuple2<>(f.getDirector(), 1));
		JavaPairRDD<String, Integer> countNames = namesMapper.reduceByKey((x, y) -> ((int) x + (int) y));

        return countNames.collect();
/*		countNames.foreach(n -> {
			//resp.getWriter().println(n.name +" "+ n.director +" "+ n.company +" "+ n.year +" "+ n.revenue);
			resp.getWriter().println(n);
		});    */
    }
}