PYTHON_JOB=$1

spark-submit --master spark://localhost:9092 --num-executors 2 \
	           --executor-memory "1G" --executor-cores 1 \
             --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta \
             $PYTHON_JOB