package lab3;

import javafx.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class DelaysInfo {

//    private JavaPairRDD<Pair<Integer, Integer>, float[]> delaysInfo;
//    private JavaPairRDD<Pair<Integer, Integer>, float[]> combineDelaysInfo;
//    private JavaPairRDD<Pair<Integer, Integer>, String> delaysInfoWritable;
//    private JavaRDD<String> delaysTable;

    private final static String COMMA = ",";

    private final static int COUNT_FLIGHT_DATA_COLUMNS = 6;
    private final static int FLIGHT_DATA_DELAY_COLUMN = 0;
    private final static int FLIGHT_DATA_CANCELED_COLUMN = 1;

//    private final static int COUNT_FLIGHTS_DATA_COLUMNS = 4;
    private final static int FLIGHTS_DATA_MAX_DELAY_COLUMN = 2;
    private final static int FLIGHTS_DATA_COUNT_DELAYS_COLUMN = 3;
    private final static int FLIGHTS_DATA_COUNT_CANCELED_COLUMN = 4;
    private final static int FLIGHTS_DATA_COUNT_FLIGHTS_COLUMN = 5;

    private final static float NULL_TIME = 0;
    private final static float NO_CANCELED = 0;
    private final static float CANCELED = 1;

    private final static int ID_FROM_COLUMN = 11;
    private final static int ID_TO_COLUMN = 14;
    private final static int DELAY_COLUMN = 17;


//    public DelaysInfo(JavaSparkContext sc, String path) {
//        delaysTable = App.deleteTitle(sc.textFile(path));
//    }

    private static String parseLineGetPos(String line, int pos) {
        return line.split(COMMA)[pos];
    }

    private static boolean isCanceled(String s) {
        return !(parseLineGetPos(s, DELAY_COLUMN).length() > 0);
    }

    public static JavaPairRDD<Pair<Integer, Integer>, float[]> parseTable(JavaRDD<String> delaysTable) {
        return delaysTable.mapToPair(
                s -> {
                    Integer idFrom = Integer.parseInt(parseLineGetPos(s, ID_FROM_COLUMN));
                    Integer idTo = Integer.parseInt(parseLineGetPos(s, ID_TO_COLUMN));
                    Pair<Integer, Integer> ids = new Pair<>(idFrom, idTo);

                    float[] flightData = new float[COUNT_FLIGHT_DATA_COLUMNS];

                    if (!isCanceled(s)) {
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

    public static JavaPairRDD<Pair<Integer, Integer>, float[]>  calcData(JavaPairRDD<Pair<Integer, Integer>, float[]> delaysInfo) {
        return delaysInfo.reduceByKey(
                (firstFlightData, secondFlightData) -> {
                    float[] flightsInfo = new float[COUNT_FLIGHTS_DATA_COLUMNS];

                    flightsInfo[FLIGHTS_DATA_MAX_DELAY_COLUMN] = Float.max(
                            firstFlightData[FLIGHT_DATA_DELAY_COLUMN],
                            secondFlightData[FLIGHT_DATA_DELAY_COLUMN]
                    );

                    if (firstFlightData[FLIGHT_DATA_DELAY_COLUMN] != NULL_TIME) {
                        flightsInfo[FLIGHTS_DATA_COUNT_DELAYS_COLUMN]++;
                    }
                    if (secondFlightData[FLIGHT_DATA_DELAY_COLUMN] != NULL_TIME) {
                        flightsInfo[FLIGHTS_DATA_COUNT_DELAYS_COLUMN]++;
                    }

                    if (firstFlightData[FLIGHT_DATA_CANCELED_COLUMN] == CANCELED) {
                        flightsInfo[FLIGHTS_DATA_COUNT_CANCELED_COLUMN]++;
                    }
                    if (secondFlightData[FLIGHT_DATA_CANCELED_COLUMN] == CANCELED) {
                        flightsInfo[FLIGHTS_DATA_COUNT_CANCELED_COLUMN]++;
                    }

                    flightsInfo[FLIGHTS_DATA_COUNT_FLIGHTS_COLUMN] += 2;

                    return flightsInfo;
                }
        );
    }

    public static JavaPairRDD<Pair<Integer, Integer>, String> toWritable(JavaPairRDD<Pair<Integer, Integer>, float[]> combineDelaysInfo) {
        return combineDelaysInfo.mapValues(
                flightsInfo -> {
                    float maxDelayTime = flightsInfo[FLIGHTS_DATA_MAX_DELAY_COLUMN];
                    float percenatageDelays = flightsInfo[FLIGHT_DATA_DELAY_COLUMN] / flightsInfo[FLIGHTS_DATA_COUNT_FLIGHTS_COLUMN] * 100;
                    float percenatageCanceled = flightsInfo[FLIGHT_DATA_CANCELED_COLUMN] / flightsInfo[FLIGHTS_DATA_COUNT_FLIGHTS_COLUMN] * 100;
                    return "   MaxDelayTime = " + maxDelayTime
                            + "; PercenatageDelays = " + percenatageDelays + "%"
                            + "; PercenatageCanceled = " + percenatageCanceled + "%";
                }
        );
    }


//    public JavaPairRDD<Pair<Integer, Integer>, String> getDelaysInfoWritable() {
//        return delaysInfoWritable;
//    }
}
