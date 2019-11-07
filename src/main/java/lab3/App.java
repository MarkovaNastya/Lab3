package lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App {

    public static JavaRDD<String> deleteTitle(JavaRDD<String> text) {
        String title = text.first();
        return text.filter(
                s -> !s.equals(title)

        );
    }


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        AirportsInfo airportsTable = new AirportsInfo(sc, args[0]);
        airportsTable.parseTable();
        JavaPairRDD<Integer, String> airportsInfo = airportsTable.getAirportsInfo();





    }
}
