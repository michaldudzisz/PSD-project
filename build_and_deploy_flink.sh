mvn clean package -f ./consumer/pom.xml

docker cp consumer/target/temperatureconsumer-0.1.jar psd-project-jobmanager-1:/temperatureconsumer-0.1.jar
docker exec -it psd-project-jobmanager-1 flink run /temperatureconsumer-0.1.jar
