FROM openjdk:8
COPY ./ consumer.jar
CMD ["java", "-jar", "producer.jar", "--request-queue=https://sqs.us-east-1.amazonaws.com/321949866576/cs5260-requests", "--max-runtime=20000"]