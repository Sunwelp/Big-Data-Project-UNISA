/opt/bitnami/spark/bin/spark-submit \
  --class it.unisa.diem.BigData.SparkDriver \
  --master local \
  ./Spark_Outlet.jar \
  ./input ./output
