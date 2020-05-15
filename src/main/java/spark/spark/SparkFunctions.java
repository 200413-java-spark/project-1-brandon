package spark.spark;

import spark.jdbc.DatabaseDML;
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
		conf.set("spark.testing.memory", "2147480000");
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

	public static JavaRDD<MovieFromCSV> initParse() {
		JavaRDD<String> entireCSV = sparkContext.textFile(new File("movies.csv").getAbsolutePath()).cache();
		//filter out headers
		String header = entireCSV.first();
		JavaRDD<String> headerlessCSV = entireCSV.filter(row -> !row.equals(header)); //.cache();
		// map to MyCSVFile data struct
        JavaRDD<MovieFromCSV> filteredHeaderlessRow = headerlessCSV.map((n) -> ParseMovie.parseCSV(n));
		return filteredHeaderlessRow;
	}

/*	public static List<Tuple2<String, Integer>> reduceByKeyName(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Integer> namesMapper = csvRDD.mapToPair((f) -> new Tuple2<>(f.getName(), 1)).reduceByKey((x, y) -> ((int) x + (int) y));
        return namesMapper.collect();
	}	*/

	public static long countName(JavaRDD<MovieFromCSV> csvRDD) {
		DatabaseDML.insertDB("countMovies", csvRDD.count());
		return csvRDD.count();
	}

	public static double boxOfficeAvg(JavaRDD<MovieFromCSV> csvRDD){
		double box = csvRDD.map((f) -> (double)f.revenue).reduce((x,y) -> (x + y))/csvRDD.count();
		DatabaseDML.insertDB("boxOfficeAve", box);
		return box;
	}

	public static List<Tuple2<Integer, Integer>> reduceByKeyYear(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<Integer, Integer> namesMapper = csvRDD.mapToPair((f) -> new Tuple2<>(f.getYear(), 1)).reduceByKey((x, y) -> ((int) x + (int) y));
		return namesMapper.collect();
	}

	public static List<Tuple2<String, Integer>> reduceByKeyCompany(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Integer> namesMapper = csvRDD.mapToPair((f) -> new Tuple2<>(f.getCompany(), 1)).reduceByKey((x, y) -> ((int) x + (int) y));
		return namesMapper.collect();
	}

	public static double countDirector(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Integer> namesMapper = csvRDD.mapToPair((f) -> new Tuple2<>(f.getDirector(), 1)).reduceByKey((x, y) -> ((int) x + (int) y));
		DatabaseDML.insertDB("countDirector", namesMapper.count());
		return namesMapper.count();
	}

	public static double countCompany(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Integer> namesMapper = csvRDD.mapToPair((f) -> new Tuple2<>(f.getCompany(), 1)).reduceByKey((x, y) -> ((int) x + (int) y));
		DatabaseDML.insertDB("countCompany", namesMapper.count());
		return namesMapper.count();
	}

	public static double countGenre(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Integer> namesMapper = csvRDD.mapToPair((f) -> new Tuple2<>(f.getGenre(), 1)).reduceByKey((x, y) -> ((int) x + (int) y));
		DatabaseDML.insertDB("countGenre", namesMapper.count());
		return namesMapper.count();
	}

	public static List<Tuple2<String, Integer>> reduceByKeyDirector(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Integer> namesMapper = csvRDD.mapToPair((f) -> new Tuple2<>(f.getDirector(), 1)).reduceByKey((x, y) -> ((int) x + (int) y));
		return namesMapper.collect();
	}

	public static List<Tuple2<Double, Integer>> averageRevenue(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<Double, Integer> namesMapper = csvRDD.mapToPair((f) -> new Tuple2<>(f.getRevenue(), 1)).reduceByKey((x, y) -> ((int) x + (int) y));
		return namesMapper.collect();
//		return data.mapToPair((f) -> new Tuple2<>(f.testPrep, 1)).reduceByKey((x, y) -> ((int) x + (int) y)).collect().toString();
	}
	
	public static List<Tuple2<String, Double>> genreAvg(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Double> namesMapper = csvRDD.mapToPair(f -> new Tuple2<>(f.genre, f.revenue))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2)
				.mapToPair(f -> f.swap()).sortByKey(false).mapToPair(f -> f.swap());
		List<Tuple2<String,Double>> var = namesMapper.collect();
		DatabaseDML.insertDB("genreAvg", var.toString());
		return var;
	}

	public static List<Tuple2<String, Double>> highRevenueMovies(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Double> namesMapper = csvRDD.mapToPair(f -> new Tuple2<>(f.name, f.revenue))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2)
				.mapToPair(f -> f.swap()).sortByKey(false).mapToPair(f -> f.swap());
		List<Tuple2<String,Double>> var = namesMapper.take(10);
		DatabaseDML.insertDB("highRevenueMovies", var.toString());
		return var;
	}

	public static List<Tuple2<String, Double>> avgRevenueDirector(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Double> namesMapper = csvRDD.mapToPair(f -> new Tuple2<>(f.director, f.revenue))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2)
				.mapToPair(f -> f.swap()).sortByKey(false).mapToPair(f -> f.swap());
		List<Tuple2<String,Double>> var = namesMapper.take(10);
		DatabaseDML.insertDB("avgRevenueDirector", var.toString());
		return var;
	}

	public static List<Tuple2<String, Double>> totalRevenueDirector(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Double> namesMapper = csvRDD.mapToPair(f -> new Tuple2<>(f.director, f.revenue))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1+f._2)
				.mapToPair(f -> f.swap()).sortByKey(false).mapToPair(f -> f.swap());
		List<Tuple2<String,Double>> var = namesMapper.take(10);
		DatabaseDML.insertDB("totalRevenueDirector", var.toString());
		return var;
	}

	public static List<Tuple2<String, Double>> totalRevenueCompany(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Double> namesMapper = csvRDD.mapToPair(f -> new Tuple2<>(f.company, f.revenue))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1+f._2)
				.mapToPair(f -> f.swap()).sortByKey(false).mapToPair(f -> f.swap());
		List<Tuple2<String,Double>> var = namesMapper.take(10);
		DatabaseDML.insertDB("totalRevenueCompany", var.toString());
		return var;
	}

	public static List<Tuple2<String, Double>> avgRevenueCompany(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Double> namesMapper = csvRDD.mapToPair(f -> new Tuple2<>(f.company, f.revenue))
				.mapValues(f -> new Tuple2<>(f,1))
				.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2))
				.mapValues(f -> f._1/f._2)
				.mapToPair(f -> f.swap()).sortByKey(false).mapToPair(f -> f.swap());
		List<Tuple2<String,Double>> var = namesMapper.take(10);
		DatabaseDML.insertDB("avgRevenueDirector", var.toString());
		return var;
	}

	public static List<Tuple2<String, Integer>> reduceByKeyGenre(JavaRDD<MovieFromCSV> csvRDD) {
		JavaPairRDD<String, Integer> namesMapper = csvRDD.mapToPair((f) -> new Tuple2<>(f.getGenre(), 1)).reduceByKey((x, y) -> ((int) x + (int) y))
				.mapToPair(f -> f.swap()).sortByKey(false).mapToPair(f -> f.swap());
		DatabaseDML.insertDB("reduceByKeyGenre", namesMapper.collect().toString());
		return namesMapper.collect();
	}

}