package lab3;

import javafx.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class DelaysInfo {

    private JavaPairRDD<Pair<Integer, Integer>, float[]> delaysInfo;
    private JavaPairRDD<Pair<Integer, Integer>, float[]> combineDelaysInfo;
    private JavaRDD<String> delaysTable;

    private final static String COMMA = ",";

    private final static int COUNT_FLIGHT_DATA_COLUMNS = 2;
    private final static int FLIGHT_DATA_DELAY_COLUMN = 0;
    private final static int FLIGHT_DATA_CANCELED_COLUMN = 1;

    private final static int COUNT_FLIGHTS_DATA_COLUMNS = 2;




    private final static float NULL_TIME = 0;
    private final static float NO_CANCELED = 0;
    private final static float CANCELED = 1;

    private final static int ID_FROM_COLUMN = 11;
    private final static int ID_TO_COLUMN = 14;
    private final static int DELAY_COLUMN = 17;
//    private final static int ID_TO_COLUMN = 14;
//    private final static int ID_TO_COLUMN = 14;
//    private final static int ID_TO_COLUMN = 14;
//    private final static int ID_TO_COLUMN = 14;


    public DelaysInfo(JavaSparkContext sc, String path) {
        delaysTable = App.deleteTitle(sc.textFile(path));
    }

    private String parseLineGetPos(String line, int pos) {
        return line.split(COMMA)[pos];
    }

    public void parseTable() {
        delaysInfo = delaysTable.mapToPair(
                s -> {
                    Integer idFrom = Integer.parseInt(parseLineGetPos(s, ID_FROM_COLUMN));
                    Integer idTo = Integer.parseInt(parseLineGetPos(s, ID_TO_COLUMN));
                    Pair<Integer, Integer> ids = new Pair<>(idFrom, idTo);

                    float flightData[] = new float[COUNT_FLIGHT_DATA_COLUMNS];

                    if (parseLineGetPos(s, DELAY_COLUMN).length() > 0) {
                        flightData[FLIGHT_DATA_DELAY_COLUMN] = Float.parseFloat(parseLineGetPos(s, DELAY_COLUMN));
                        flightData[FLIGHT_DATA_CANCELED_COLUMN] = NO_CANCELED;
                    } else {
                        flightData[FLIGHT_DATA_DELAY_COLUMN] = NULL_TIME;
                        flightData[FLIGHT_DATA_CANCELED_COLUMN] = CANCELED;
                    }

                    return new Tuple2<>(ids, flightData);
                }
        );
    }

    public void calcData() {
        combineDelaysInfo = delaysInfo.reduceByKey(
                (firstFlightData, secondFlightData) -> {

                }
        )
    }

}
