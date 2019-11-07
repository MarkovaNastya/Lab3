package lab3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Airports {

    private JavaPairRDD<Integer, String> airportsInfo;
    private JavaRDD<String> airportsTable;

    public Airports(JavaSparkContext sc, String path) {
        JavaRDD<String> airportsTable = App.deleteTitle(sc.textFile(path));
    }

    private String parseLine(String line) {
        
    }

    private JavaPairRDD<Integer, String> parseTable() {
        airportsTable.mapToPair(
                s -> {
                    Integer id = s.split(",", 2)[0];

                    return Tuple2<>;
                }
        );

    }



}
