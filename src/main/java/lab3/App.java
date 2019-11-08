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

    public static JavaRDD<String> convertAllDataTo(Broadcast<Map<Integer, String>> airportsBroadcasted,
                             JavaPairRDD<Pair<Integer, Integer>, String> delaysInfo) {

        return delaysInfo.map(
                data -> {
                    Integer id1 = data._1.getKey();
                    Integer id2 = data._1.getValue();
                    String delaysInfoString = data._2;

                    String id1desc = airportsBroadcasted.getValue().get(id1);
                    String id2desc = airportsBroadcasted.getValue().get(id2);

                    String out = "From " + id1desc + " (" + id1 + ") "
                            + "to " + id2desc + " (" + id2 + ") "
                            + delaysInfoString;

                    return out;
                }
        );
    }JA


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("App");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airportsTable = deleteTitle(sc.textFile(args[0]));
        JavaPairRDD<Integer, String> airportsInfo = AirportsFunctions.parseTable(airportsTable);
        Map<Integer, String> airportsInfoMap = airportsInfo.collectAsMap();

        final Broadcast<Map<Integer, String>> airportsBroadcasted = sc.broadcast(airportsInfoMap);

        JavaRDD<String> delaysTable = deleteTitle(sc.textFile(args[1]));
        JavaPairRDD<Pair<Integer, Integer>, float[]> parseTable = DelaysFunctions.parseTable(delaysTable);
        JavaPairRDD<Pair<Integer, Integer>, float[]> calcTable = DelaysFunctions.calcData(parseTable);
        JavaPairRDD<Pair<Integer, Integer>, String> delaysInfo = DelaysFunctions.convertToWritable(calcTable);

        JavaRDD<String> out = convertAllDataTo(airportsBroadcasted, delaysInfo);

        out.saveAsTextFile(args[2]);

    }
}
