package lab3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DelaysInfo {

    private JavaPairRDD<Integer, Integer> delaysInfoIDs;
    private float delaysInfoData[];
    private JavaRDD<String> delaysTable;

    private final static int COUNT_DATA_COLUMNS = 1;

    public DelaysInfo(JavaSparkContext sc, String path) {
        delaysTable = App.deleteTitle(sc.textFile(path));
        delaysInfoData = new float[COUNT_DATA_COLUMNS];
    }

    public void parseTable() {
        
    }

}
