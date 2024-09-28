#Import necessary Libraries
import boto3
from botocore.exceptions import ClientError

#Create S3, SQS and rekognition client objects and loop through try loop to get client error :
try :
    s3 = boto3.resource('s3', region_name = 'us-east-1')
except ClientError as e:
    print(f"Error creating S3 client: {e}")

try:
    sqs = boto3.client('sqs', region_name = 'us-east-1')
except ClientError as e:
    print(f"Error creating SQS client: {e}")

try:
    rekognition = boto3.client('rekognition', region_name = 'us-ease-1')
except ClientError as e:
    print(f"Error creating Rekognition client: {e}")


#Define queue and bucket name :
queue_url = 'https://sqs.us-east-1.amazonaws.com/590183997993/image-recognition-demo'
bucket_name = 'my-image-recognition-demo-bucket'


#Loop continuously to proecess each message from SQS
while True:
    #Receive message from SQS queue stored by Instance 1
    response = sqs.receive_message(
        QueueUrl = queue_url,
        AttributeNames = ['All'],
        MaxNumberofMessages = 1,
        WaitTimeSeconds = 20
    )


    #Break out of the loop if no messages received
    if 'Messages' not in response:
        break


    message= response['Message'][0]
    receipt_handle = message['ReceiptHandle']
    message_body = message['Body']


    #Delete the message if the message body is -1 and terminate teh process
    if message_body=='-1' and 'Messages' not in sqs.receive_message(QueueUrl = queue_url, AttributeNames=['All'], MaxNumberofMessages = 1):
        sqs.delete_message(QueueUrl = queue_url, ReceiptHandle  = receipt_handle)
        break

    #Get Object key and check if the file extension is supported
    obj_key = message_body
    file_extension = obj_key.split('.')[-1].lower()
    if file_extension not in ['jpg', 'jpeg', 'png']:
        print(f"{obj_key} has unsupported file extension")
        sqs.delete_message(QueueUrl = queue_url, ReceiptHandle = receipt_handle)
        continue

    #Read the image Object from S3
    try:
        image_object = s3.Object(bucket_name, obj_key)
        image_content = image_object.get()['Body'].read()
    except ClientError as e:
        print(f"Error reading Object {obj_key} from S3 : {e}")
        sqs.delete_message(QueueUrl = queue_url, ReceiptHandle=receipt_handle)
        continue

    #Detect Text in the image using Rekognition
    try:
        respone = rekognition.detect_text(
            Image = {
                'Bytes' : image_content
            }  
        )
    except ClientError as e:
        print(f"Error reading Object {obj_key} from S3 : {e}")
        sqs.delete_message(QueueUrl = queue_url, ReceiptHandle=receipt_handle)
        continue

    #Extract detected text from the response
    detected_text = ""
    for text in response['TextDetections']:
        if text['Type']=='LINE':
            detected_text+=text['DetectedText'] + ""

    
    #Write Detected text to output file if both car and text are detected
    if detected_text.strip():
        with open('output.txt', 'a') as f:
            f.write(f"Car and text detected in {obj_key} : {detected_text}\n")
            print(f"Detected text in {obj_key} : {detected_text}")
    else:
        print(f"No Car or text detected in {obj_key}")


    #Delete message from sqs once each message is processed
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

print("All image processed")








