package lab3;

import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Map;

public class App {

    public static JavaRDD<String> deleteTitle(JavaRDD<String> text) {
        String title = text.first();
        return text.filter(
                s -> !s.equals(title)

        );
    }

    public static String map(Broadcast<Map<Integer, String>> airportsBroadcasted,
                             JavaPairRDD<Pair<Integer, Integer>, String> delaysInfo) {
        
    }


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        AirportsInfo airportsTable = new AirportsInfo(sc, args[0]);
        airportsTable.parseTable();
        JavaPairRDD<Integer, String> airportsInfo = airportsTable.getAirportsInfo();
        Map<Integer, String> airportsInfoMap = airportsInfo.collectAsMap();

        final Broadcast<Map<Integer, String>> airportsBroadcasted = sc.broadcast(airportsInfoMap);

        DelaysInfo delaysTable = new DelaysInfo(sc, args[1]);
        delaysTable.parseTable();
        delaysTable.calcData();
        delaysTable.toWritable();
        JavaPairRDD<Pair<Integer, Integer>, String> delaysInfo = delaysTable.getDelaysInfoWritable();




    }
}
