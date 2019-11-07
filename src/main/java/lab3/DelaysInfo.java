package lab3;

import javafx.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DelaysInfo {

    private JavaPairRDD<Pair<Integer, Integer>, float[]> delaysInfo;
    private JavaRDD<String> delaysTable;

    private final static String COMMA = ",";

    private final static int COUNT_DATA_COLUMNS = 1;

    private final static int ID_FROM_COLUMN = 11;
    private final static int ID_TO_COLUMN = 14;
    private final static int ID_TO_COLUMN = 14;
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

                    float data[] = new float[COUNT_DATA_COLUMNS];





                }
        );

    }

}
