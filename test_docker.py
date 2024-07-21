import requests 

event = {
    "Records": [
        {
            "kinesis": {
                "kinesisSchemaVersion": "1.0",
                "partitionKey": "1",
                "sequenceNumber": "49630081666084879290581185630324770398608704880802529282",
                "data": "eyJSX0NsYWltc19oaXN0b3J5IjogMC4wLCAiVHlwZV9yaXNrIjogIjMiLCAiQXJlYSI6IDEsICJTZWNvbmRfZHJpdmVyIjogMSwgIlBvd2VyIjogMTIwLCAiQ3lsaW5kZXJfY2FwYWNpdHkiOiAxNjAwLCAiVmFsdWVfdmVoaWNsZSI6IDEyMDAwLCAiVHlwZV9mdWVsIjogIlAiLCAiYXZfc2VuaW9yX3llYXIiOiAwLjUsICJjdXN0b21lcl9hZ2UiOiAyOC4zMywgImRyaXZpbmdfbGljZW5jZV9sb25nIjogOS45OCwgInZlaGljbGVfYWdlIjogMTAsICJwb2xpY3lfc3RhYmlsaXR5IjogMS4wLCAicHJvZHVjdF9zdGFiaWxpdHkiOiAxLjAsICJjbGFpbXNfcGVyX3BvbGljeSI6IDAuMCwgImNvc3RfcGVyX2NsYWltIjogMC4wfQ==",
                "approximateArrivalTimestamp": 1654161514.132
            },
            "eventSource": "aws:kinesis",
            "eventVersion": "1.0",
            "eventID": "shardId-000000000000:49630081666084879290581185630324770398608704880802529282",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::XXXXXXXXX:role/lambda-kinesis-role",
            "awsRegion": "eu-west-1",
            "eventSourceARN": "arn:aws:kinesis:eu-west-1:XXXXXXXXX:stream/ride_events"
        }
    ]
}

url = 'http://localhost:8080/2015-03-31/functions/function/invocations'
response = requests.post(url, json=event)
print(response.json())