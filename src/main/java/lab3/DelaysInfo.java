package lab3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DelaysInfo {

    public AirportsInfo(JavaSparkContext sc, String path) {
        JavaRDD<String> airportsTable = App.deleteTitle(sc.textFile(path));
    }

}
