#!/bin/bash

# --- CẤU HÌNH BIẾN ---
JAR_NAME="ex2.jar"
MAIN_CLASS="ex2"
JAVA_FILE="ex2.java"
HDFS_INPUT="/input/ratings_*.txt"
HDFS_OUTPUT="/output/ex2_result"

echo "--- BẮT ĐẦU QUY TRÌNH HADOOP ---"

# 1. Xóa thư mục output cũ trên HDFS
echo "1. Đang xóa output cũ: $HDFS_OUTPUT"
hdfs dfs -rm -r $HDFS_OUTPUT 2>/dev/null

# 2. Biên dịch code Java
echo "2. Đang biên dịch $JAVA_FILE..."
javac -classpath $(hadoop classpath) -d . $JAVA_FILE

# Kiểm tra nếu biên dịch lỗi thì dừng lại luôn
if [ $? -ne 0 ]; then
    echo "LỖI: Biên dịch thất bại!"
    exit 1
fi

# 3. Đóng gói JAR
echo "3. Đang tạo file $JAR_NAME..."
jar -cvf $JAR_NAME *.class

# 4. Chạy Job MapReduce
echo "4. Đang gửi Job lên Hadoop..."
hadoop jar $JAR_NAME $MAIN_CLASS $HDFS_INPUT $HDFS_OUTPUT

# 5. Kiểm tra kết quả ngay sau khi chạy xong
echo "--- KẾT QUẢ CUỐI CÙNG ---"
hdfs dfs -ls $HDFS_OUTPUT
hdfs dfs -cat $HDFS_OUTPUT/part-r-00000 