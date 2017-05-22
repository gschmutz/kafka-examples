kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test.partitions

echo "A,test1" | kafka-console-producer \
            --broker-list localhost:9092 \
            --topic test.partitions \
            --property parse.key=true \
            --property key.separator=, 

echo "A,test2" | kafka-console-producer \
            --broker-list localhost:9092 \
            --topic test.partitions \
            --property parse.key=true \
            --property key.separator=, 

echo "A,test3" | kafka-console-producer \
            --broker-list localhost:9092 \
            --topic test.partitions \
            --property parse.key=true \
            --property key.separator=, 

kafka-console-consumer \
            --bootstrap-server localhost:9092 \
            --topic test.partitions \
            --partition 0 \
            --property key.print=true \
            --new-consumer

kafka-topics --alter --zookeeper localhost:2181 --partitions 3 --topic test.partitions

