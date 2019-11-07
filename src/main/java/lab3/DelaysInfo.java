package lab3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DelaysInfo {

    private JavaPairRDD<Integer, String> airportsInfo;
    private JavaRDD<String> airportsTable;

    public DelaysInfo(JavaSparkContext sc, String path) {
        delaysTable = App.deleteTitle(sc.textFile(path));
    }

}
