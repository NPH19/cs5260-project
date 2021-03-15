import sys
import logging
import boto3
import json
from time import sleep

class consumer():

    def __init__(self, store_strategy, bucket_2):
        self.store_strategy = store_strategy
        self.bucket_2 = bucket_2

    def WidgetCreateRequest():
        pass
    
    def WidgetDeleteRequest():
        pass
    
    def WidgetChangeRequest():
        pass
    
    
def main():
    if len(sys.argv) != 5:
        print('python <filename>.py <name of request bucket> <\"s3\" or \"DynamoDB\"> <name of s3 or DB> <max time to run>')
        sys.exit()
    for arg in sys.argv[1:]:
        print(arg)
    
    
    s3client = boto3.client("s3")
    
    widget_json = s3client.get_object(Bucket='usu-cs-5260-hud-requests', Key='1612306368338')
    widget = json.loads(widget_json)
    
    
    

if __name__ == "__main__":
    main()