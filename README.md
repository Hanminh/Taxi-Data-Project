# Hướng dẫn sử dụng
1. Run pipeline bằng Jenkins:
   - Truy cập url của jenkins server (ví dụ: http://3.81.102.21:8080/), chọn job 'Bigdata-pipeline' từ dashboard và click vào 'build job'
   - Ngoài ra, có thể cấu hình trigger như: Build Periodically (Trigger theo thời gian), Trigger Remotely (Kích hoạt job từ xa bằng HTTP API), ...

2. Build và run các Docker containers:
   ```
   docker-compose up --build
   ```
   - Với Kafka:
     Producer chạy file kafka_producer.py để sinh dữ liệu và gửi vào Kafka:
     ```
     python kafka_producer.py 
     ```
     Batch Consumer lưu dữ liệu từ Kafka vào HDFS:
     ```
     python kafka_consumer.py 
     ```
   - Với Spark:
     Xử lý dữ liệu và lưu vào Cassandra
     ```
     python spark/passenger_statistic.py
     python spark/payment_type_statistic.py
     python spark/peak_pickup_hour_statistic.py
     python spark/total_amount_per_day_statistic.py
     ```
# Truy cập Grafana dashboard:
  Mở trình duyệt và tuy cập [http://localhost:3000](http://localhost:3000/)
