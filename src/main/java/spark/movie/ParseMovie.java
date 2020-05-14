package spark.movie;

public class ParseMovie {
    
    public static MovieFromCSV parseCSV(String s) {
        String[] output = new String[6];
        String[] toParse = s.split(",");

        output[0] = toParse[0];
        output[1] = toParse[1];
        output[2] = toParse[2];
        output[3] = toParse[3];
        output[4] = toParse[4];
        output[5] = toParse[5];

        MovieFromCSV movie = new MovieFromCSV(output);
        return movie;
    }
}