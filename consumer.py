## github link: https://github.com/NPH19/cs5260-hw2

import sys
import logging
import boto3
import json
from time import sleep, time

class consumerClass():

    def __init__(self, store_strategy, store_name, request_bucket):
        self.store_strategy = store_strategy
        self.s3client = boto3.client("s3")
        self.request_bucket = request_bucket
        self.store_name = store_name
        self.s3resource = boto3.resource('s3')
        self.widget = None

    def WidgetCreateRequest(self):
        print('create ',self.widget)
        if self.store_strategy == 's3':
            key = 'widgets/'+str(self.widget['owner'])+'/'+str(self.widget['widgetId'])
            self.s3client.put_object(Bucket=self.store_name, Key=key, Body=str(self.widget))
            logging.info(str('key: '+key+' added successfully to s3 bucket'))
            
        elif self.store_strategy == 'DynamoDB':
            table_dict = {}
            table_dict['widgetId'] = self.widget['widgetId']
            table_dict['owner'] = self.widget['owner']
            table_dict['label'] = self.widget['label']
            table_dict['description'] = self.widget['description']
            table_dict['other'] = self.widget['otherAttributes']
            
            DynoDB = boto3.resource('dynamodb').Table(self.store_name).put_item(Item=table_dict)
            logging.info(str('widgetId: '+self.widget['widgetId']+' added successfully to DynamoDB table'))
            
        else:
            logging.warn('create widget did not add to s3 or DynamoDB')
            print('no good')
    
    def WidgetDeleteRequest(self):
        print('delete ',self.widget)
        logging.info("Delete Request Encountered")
        if self.store_strategy == 's3':
            key = 'widgets/'+str(self.widget['owner'])+'/'+str(self.widget['widgetId'])
            self.s3resource.Object(str(self.store_name), str(key)).delete
        if self.store_strategy == 'DynamoDB':
            table_dict = {}
            table_dict['widgetId'] = self.widget['widgetId']
            table_dict['owner'] = self.widget['owner']
            
            DynoDB = boto3.resource('dynamodb').Table(self.store_name).delete_item(Key=table_dict)
            
    
    def WidgetChangeRequest(self):
        print('update ',self.widget)
        if self.store_strategy == 's3':
            self.WidgetCreateRequest()
        logging.info("Change Request Encountered")
        
    def sortWidget(self, widget):
        self.widget = widget
        if widget['type'] == 'create':
            self.WidgetCreateRequest()
        elif widget['type'] == 'delete':
            self.WidgetDeleteRequest()
        elif widget['type'] == 'update':
            self.WidgetChangeRequest()
        else:
            logging.warning("Request was not handled!")
    
    
def main():
    start = time()
    if len(sys.argv) != 6:
        print("ERROR: args did not match expected (see below)")
        print('python <filename>.py <name of request bucket or url of sqs> <\"s3\" or \"DynamoDB\"> <name of s3 or DB> <max time to run (seconds)> <S3 or SQS>')
        #python consumer.py usu-cs5260-hud-requests s3 usu-cs5260-hud-web 1
        # python consumer.py https://sqs.us-east-1.amazonaws.com/321949866576/cs5260-requests s3 usu-cs5260-hud-web 1 SQS
        # python consumer.py https://sqs.us-east-1.amazonaws.com/321949866576/cs5260-requests DynamoDB widgets 1 SQS
        sys.exit()
    req_bucket = sys.argv[1]
    store_strategy = sys.argv[2]
    store_name = sys.argv[3]
    time_to_run = int(sys.argv[4])
    source = sys.argv[5]
    
    consumer = consumerClass(store_strategy, store_name, req_bucket)
    if source == 'S3':
        s3resource = boto3.resource("s3")
        request_bucket = s3resource.Bucket(req_bucket)
        s3client = boto3.client("s3")
        
    if source == 'SQS':
        sqs = boto3.client('sqs')
        queue_url = req_bucket
        
    
    while time()-start <= time_to_run:
        if source == 'S3':
            request_list = request_bucket.objects.all()
            size = sum(1 for _ in request_list)
            if size > 0:
                for request in request_list:
                    key = request.key
                    print(request.key)
                    widget_response = s3client.get_object(Bucket=req_bucket, Key=key)
                    widget_stream = widget_response['Body']
                    widget = json.load(widget_stream)
                    s3client.delete_object(Bucket=req_bucket, Key=key)
                    
                    consumer.sortWidget(widget)
    
        elif source == 'SQS':
            # Handle requests
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=[
                    'SentTimestamp'
                ],
                MaxNumberOfMessages=1,
                MessageAttributeNames=[
                    'All'
                ],
                VisibilityTimeout=0,
                WaitTimeSeconds=0
            )

            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            widget_stream = message['Body']
            widget = json.loads(widget_stream)
            
            consumer.sortWidget(widget)
            
            # TODO: delete queue item
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
        
        else:
            logging.error("request source not specified correctly")
        
    print("finished running for given time")
    sys.exit()
    
    

if __name__ == "__main__":
    main()