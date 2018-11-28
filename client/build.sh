#Build the project with the container using Maven.
docker run --rm -v $(pwd)/project:/project  --entrypoint mvn  buildhiveclient:dev clean package -f /project/pom.xml

