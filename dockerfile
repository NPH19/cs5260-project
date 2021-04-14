FROM python:3.8-slim-buster
RUN pip3 install boto3
COPY ./consumer.py consumer.py
CMD ["python", "consumer.py", "https://sqs.us-east-1.amazonaws.com/321949866576/cs5260-requests", "s3", "usu-cs5260-hud-web", "2", "SQS"]