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

    def WidgetCreateRequest(self, widget):
        if self.store_strategy == 's3':
            key = 'widgets/'+str(widget['owner'])+'/'+str(widget['widgetId'])
            self.s3client.put_object(Bucket=self.store_name, Key=key, Body=str(widget))
            logging.info('key: ',key,' added successfully')
        else:
            logging.warn('create widget did not add to s3 or DynamoDB')
            print('no good')
    
    def WidgetDeleteRequest():
        logging.info("Delete Request Encountered")
    
    def WidgetChangeRequest():
        logging.info("Change Request Encountered")
    
    
def main():
    start = time()
    if len(sys.argv) != 5:
        print("ERROR: args did not match expected (see below)")
        print('python <filename>.py <name of request bucket> <\"s3\" or \"DynamoDB\"> <name of s3 or DB> <max time to run (seconds)>')
        #python consumer.py usu-cs5260-hud-requests s3 usu-cs5260-hud-web 1
        sys.exit()
    req_bucket = sys.argv[1]
    store_strategy = sys.argv[2]
    store_name = sys.argv[3]
    time_to_run = int(sys.argv[4])
    
    consumer = consumerClass(store_strategy, store_name, req_bucket)
    s3resource = boto3.resource("s3")
    request_bucket = s3resource.Bucket(req_bucket)
    s3client = boto3.client("s3")
    
    # while time()-start <= time_to_run:
    request_list = request_bucket.objects.all()
    size = sum(1 for _ in request_list)
    if size > 0:
        for request in request_list:
            key = request.key
            print(request.key)
            widget_response = s3client.get_object(Bucket=req_bucket, Key=key)
            widget_stream = widget_response['Body']
            widget = json.load(widget_stream)
            # s3client.delete_object(Bucket=req_bucket, Key=key)
            
            if widget['type'] == 'create':
                consumer.WidgetCreateRequest(widget)
            elif widget['type'] == 'delete':
                consumer.WidgetDeleteRequest(widget)
            elif widget['type'] == 'change':
                consumer.WidgetChangeRequest(widget)
            else:
                logging.warning("Request was not handled!")
        
    print("finished running for given time")
    sys.exit
    
    

if __name__ == "__main__":
    main()