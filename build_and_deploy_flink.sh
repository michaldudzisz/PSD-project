mvn clean package -f ./consumer/pom.xml

docker cp consumer/target/temperatureconsumer-0.1.jar psd-lab-3-jobmanager-1:/temperatureconsumer-0.1.jar
docker exec -it psd-lab-3-jobmanager-1 flink run /temperatureconsumer-0.1.jar
