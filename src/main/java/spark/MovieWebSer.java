package spark;

import spark.spark.SparkFunctions;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet("/movie")
public class MovieWebSer extends HttpServlet {
	
	@Override
	public void init() throws ServletException{
		SparkFunctions.initSpark();

	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
//		resp.getWriter().println(SparkFunctions.reduceByKeyArray());
//		resp.getWriter().println(SparkFunctions.reduceByKeySimpleCSVTest());
		resp.getWriter().println(SparkFunctions.reduceByKeyCSVMovie());

	}
}