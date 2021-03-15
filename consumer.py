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

    def WidgetCreateRequest(self, request):
        if self.store_strategy == 's3':
            # widget_response = self.s3client.get_object(Bucket=self.request_bucket, Key=key)
            # widget_dict = widget_response['ResponseMetadata']
            # # widget = json.loads(str(widget_dict))
            # print(widget_dict['owner'])
            print(type(request))
        else:
            print('no good')
    
    def WidgetDeleteRequest():
        pass
    
    def WidgetChangeRequest():
        pass
    
    
def main():
    start = time()
    if len(sys.argv) != 5:
        print("ERROR: args did not match expected (see below)")
        print('python <filename>.py <name of request bucket> <\"s3\" or \"DynamoDB\"> <name of s3 or DB> <max time to run (seconds)>')
        sys.exit()
    request_bucket = sys.argv[1]
    store_strategy = sys.argv[2]
    store_name = sys.argv[3]
    time_to_run = sys.argv[4]
    
    consumer = consumerClass(store_strategy, store_name, request_bucket)
    
    while time()-start <= time_to_run:
        request_list = request_bucket.objects.all()
            for request in request_list:
                consumer.WidgetCreateRequest(request)

    print("finished running for given time")
    sys.exit
    
    

if __name__ == "__main__":
    main()