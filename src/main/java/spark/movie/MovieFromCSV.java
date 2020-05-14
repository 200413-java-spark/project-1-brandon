package spark.movie;

import java.io.Serializable;

public class MovieFromCSV implements Serializable {
    public String name;
    public String director;
    public String company;
    public String genre;
    public int year;
    public double revenue;

    public MovieFromCSV (String name, String director, String company, String genre, String year, String revenue) {
        this.name = name;
        this.director = director;
        this.company = company;
        this.genre = genre;
        this.year = Integer.parseInt(year);
        this.revenue = Double.parseDouble(revenue);
    }

    public MovieFromCSV (String[] s) {
        this.name = s[0];
        this.director = s[1];
        this.company = s[2];
        this.genre = s[3];
        this.year = Integer.parseInt(s[4]);
        this.revenue = Double.parseDouble(s[5]);
    }

    public String getDirector() {
        return this.director;
    }
}