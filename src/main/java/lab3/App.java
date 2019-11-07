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

    public static JavaRDD<String> map(Broadcast<Map<Integer, String>> airportsBroadcasted,
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


    }


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("App");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        AirportsInfo airportsTable = new AirportsInfo(sc, args[0]);
        JavaRDD<String> airportsTable = deleteTitle(sc.textFile(args[0]));
//        airportsTable.parseTable();
        JavaPairRDD<Integer, String> airportsInfo = AirportsInfo.parseTable(airportsTable); //airportsTable.getAirportsInfo();
        Map<Integer, String> airportsInfoMap = airportsInfo.collectAsMap();

        final Broadcast<Map<Integer, String>> airportsBroadcasted = sc.broadcast(airportsInfoMap);

//        DelaysInfo delaysTable = new DelaysInfo(sc, args[1]);
        JavaRDD<String> delaysTable = deleteTitle(sc.textFile(args[1]));
        JavaPairRDD<Pair<Integer, Integer>, float[]> parseTable = DelaysInfo.parseTable(delaysTable);
//        delaysTable.parseTable();
        JavaPairRDD<Pair<Integer, Integer>, float[]> calcTable = DelaysInfo.calcData(parseTable);
//        delaysTable.calcData();
        JavaPairRDD<Pair<Integer, Integer>, String> delaysInfo = DelaysInfo.toWritable(calcTable);
//        delaysTable.toWritable();
//        JavaPairRDD<Pair<Integer, Integer>, String> delaysInfo = delaysTable.getDelaysInfoWritable();

        JavaRDD<String> out = map(airportsBroadcasted, delaysInfo);

        out.saveAsTextFile(args[2]);

    }
}
