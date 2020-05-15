package spark;

import spark.spark.SparkFunctions;
import spark.jdbc.DatabaseDML;
import spark.movie.MovieFromCSV;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.spark.api.java.JavaRDD;

@WebServlet("/movie")
public class MovieWebSer extends HttpServlet {
	JavaRDD<MovieFromCSV> csvRDD;
	
	@Override
	public void init() throws ServletException{
		SparkFunctions.initSpark();
		csvRDD = SparkFunctions.initParse();
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
//		resp.getWriter().println(SparkFunctions.reduceByKeyArray());
//		resp.getWriter().println(SparkFunctions.reduceByKeySimpleCSVTest());
		
		resp.getWriter().println("\nWelcome to the Movie Dataset Application!!");
		
		//DatabaseDML.insertDB();

		resp.getWriter().println("\nNumber of Movies in the dataset:");
		resp.getWriter().println(SparkFunctions.countName(csvRDD));

		resp.getWriter().println("\nNumber of Directors in the dataset:");
		resp.getWriter().println(SparkFunctions.countDirector(csvRDD));

		resp.getWriter().println("\nNumber of Companies in the dataset:");
		resp.getWriter().println(SparkFunctions.countCompany(csvRDD));

		resp.getWriter().println("\nNumber of Genre in the dataset:");
		resp.getWriter().println(SparkFunctions.countGenre(csvRDD));

		resp.getWriter().println("\nAverage Movie Gross Income:");
		resp.getWriter().println(SparkFunctions.boxOfficeAvg(csvRDD));

		resp.getWriter().println("\nNumber of movies for each Genre:");
		resp.getWriter().println(SparkFunctions.reduceByKeyGenre(csvRDD));

		resp.getWriter().println("\nGenre average Gross Income:");
		resp.getWriter().println(SparkFunctions.genreAvg(csvRDD));

		resp.getWriter().println("\nTop 10 Highest Gross Income:");
		resp.getWriter().println(SparkFunctions.highRevenueMovies(csvRDD));

		resp.getWriter().println("\nTop 10 Highest Gross Income Directors:");
		resp.getWriter().println(SparkFunctions.totalRevenueDirector(csvRDD));

		resp.getWriter().println("\nTop 10 Average Highest Gross Income Directors:");
		resp.getWriter().println(SparkFunctions.avgRevenueDirector(csvRDD));

		resp.getWriter().println("\nTop 10 Highest Gross Income Companies:");
		resp.getWriter().println(SparkFunctions.totalRevenueCompany(csvRDD));

		resp.getWriter().println("\nTop 10 Average Highest Gross Income Company:");
		resp.getWriter().println(SparkFunctions.avgRevenueCompany(csvRDD));

		/*		resp.getWriter().println("\nNumber of movies for each Director:");
		resp.getWriter().println(SparkFunctions.reduceByKeyDirector(csvRDD));

		resp.getWriter().println("\nNumber of movies for each Company:");
		resp.getWriter().println(SparkFunctions.reduceByKeyCompany(csvRDD));

		resp.getWriter().println("\nNumber of movies in each Year:");
		resp.getWriter().println(SparkFunctions.reduceByKeyYear(csvRDD));

		resp.getWriter().println("\nRevenue Average:");
		resp.getWriter().println(SparkFunctions.averageRevenue(csvRDD));
*/
	}
}