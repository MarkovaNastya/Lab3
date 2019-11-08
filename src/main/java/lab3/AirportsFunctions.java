package lab3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class AirportsInfo {

//    private JavaPairRDD<Integer, String> airportsInfo;
//    private JavaRDD<String> airportsTable;

    private final static String QUOTES = "\"";
    private final static String COMMA = ",";
    private final static String EMPTY = "";

    private final static int COUNT_COLUMNS = 2;

    private final static int AIRPORTS_ID_COLUMN = 0;
    private final static int AIRPORTS_DESCRIPTION_COLUMN = 1;


//    public AirportsInfo(JavaSparkContext sc, String path) {
//        airportsTable = App.deleteTitle(sc.textFile(path));
//    }

    private static String parseLineGetPos(String line, int pos) {
        String column = line.split(COMMA, COUNT_COLUMNS)[pos];
        return column.replaceAll(QUOTES, EMPTY);
    }

    public static JavaPairRDD<Integer, String> parseTable(JavaRDD<String> airportsTable) {
        return airportsTable.mapToPair(
                s -> {
                    Integer id = Integer.parseInt(parseLineGetPos(s, AIRPORTS_ID_COLUMN));
                    String description = parseLineGetPos(s, AIRPORTS_DESCRIPTION_COLUMN);

                    return new Tuple2<>(id, description);
                }
        );
 //       (10001, "Afognak Lake, AK: Afognak Lake Airport")
//        (10003, "Granite Mountain, AK: Bear Creek Mining Strip")

    }

//    public JavaPairRDD<Integer, String> getAirportsInfo() {
//        return airportsInfo;
//    }
}