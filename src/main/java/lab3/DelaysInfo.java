package lab3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DelaysInfo {



    public DelaysInfo(JavaSparkContext sc, String path) {
        JavaRDD<String> delaysTable = App.deleteTitle(sc.textFile(path));
    }

}
