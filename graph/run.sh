./bin/spark-submit --master "local"  \
--driver-class-path guava-14.0.jar \
--driver-library-path guava-14.0.jar \
--conf spark.executor.extraClassPath=guava-14.0.jar \
--conf spark.executor.extraLibraryPath=guava-14.0.jar  \
--class com.vesoft.nebula.algorithm.Main graph-algorithm.jar \
-p application.conf