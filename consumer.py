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
            dict_to_load_into_db = {}
            dict_to_load_into_db['widgetId'] = self.widget['widgetId']
            dict_to_load_into_db['owner'] = self.widget['owner']
            dict_to_load_into_db['label'] = self.widget['label']
            dict_to_load_into_db['description'] = self.widget['description']
            dict_to_load_into_db['other'] = self.widget['otherAttributes']
            
            DynoDB = boto3.resource('dynamodb').Table(self.store_name).put_item(Item=dict_to_load_into_db)
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
            dict_to_load_into_db = {}
            dict_to_load_into_db['widgetId'] = self.widget['widgetId']
            dict_to_load_into_db['owner'] = self.widget['owner']
            
            DynoDB = boto3.resource('dynamodb').Table(self.store_name).delete_item(Key=dict_to_load_into_db)
            
    
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
            
class MessageRetriever():
    def __init__(self, source, location):
        self.request_source = source
        self.request_source_location = location
        if self.request_source == 'S3':
            self.client = boto3.client("s3")
            self.resource = boto3.resource('s3').Bucket(location)
        elif self.request_source == 'SQS':
            self.client = boto3.client('sqs', region_name='us-east-1')
        else:
            logging.error('client not setup correctly')
            
    
    def RetrieveNextRequest(self):
        if self.request_source == 'S3':
            return self.GetNextRequestFromS3()
        elif self.request_source == 'SQS':
            return self.GetNextRequestFromQueue()
        else:
            logging.error('setupRetriever() did not process correctly')
            return
           
    def GetNextRequestFromS3(self):
        request_list = self.resource.objects.all() # grab only 10 at a time
        size = sum(1 for _ in request_list)
        while size < 1:
            sleep(1)
        for request in request_list:
            key = request.key
            print(request.key)
            widget_response = self.client.get_object(Bucket=self.request_source_location, Key=key)
            widget_stream = widget_response['Body']
            widget = json.load(widget_stream)
            self.client.delete_object(Bucket=self.request_source_location, Key=key)
            break
        return widget
    
    def GetNextRequestFromQueue(self):
        response = self.client.receive_message(
            QueueUrl=self.request_source_location,
            AttributeNames=['SentTimestamp'],
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
            VisibilityTimeout=0,
            WaitTimeSeconds=0)
        # print('response: ',response)
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        widget_stream = message['Body']
        widget = json.loads(widget_stream)
        
        # TODO: delete queue item
        self.client.delete_message(
            QueueUrl=self.request_source_location,
            ReceiptHandle=receipt_handle)
            
        return widget
        
    
    
def main():
    start = time()
    if len(sys.argv) != 6:
        print("ERROR: args did not match expected (see below)")
        print('python <filename>.py <name of request bucket or url of sqs> <\"s3\" or \"DynamoDB\"> <name of s3 or DB> <max time to run (seconds)> <S3 or SQS>')
        # python consumer.py usu-cs5260-hud-requests s3 usu-cs5260-hud-web 1
        # python consumer.py https://sqs.us-east-1.amazonaws.com/321949866576/cs5260-requests s3 usu-cs5260-hud-web 1 SQS
        # python consumer.py https://sqs.us-east-1.amazonaws.com/321949866576/cs5260-requests DynamoDB widgets 1 SQS
        sys.exit()
    req_location = sys.argv[1]
    store_strategy = sys.argv[2]
    store_name = sys.argv[3]
    time_to_run = int(sys.argv[4])
    request_source = sys.argv[5]
    
    consumer = consumerClass(store_strategy, store_name, req_location)
    retreiver = MessageRetriever(request_source, req_location)

    while time()-start <= time_to_run:
        
        widget = retreiver.RetrieveNextRequest()          
        consumer.sortWidget(widget)

            
    print("finished running for given time")
    sys.exit()
    
    

if __name__ == "__main__":
    main()