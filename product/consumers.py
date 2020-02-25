import channels.layers
import json
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr
from asgiref.sync import async_to_sync
from channels.generic.websocket import WebsocketConsumer, JsonWebsocketConsumer

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('realtime_product')


class ProductConsumer(JsonWebsocketConsumer):

    def connect(self):
        self.room_name = 'product'
        self.room_group_name = 'product_%s' % self.room_name

        async_to_sync(self.channel_layer.group_add)(
            self.room_group_name,
            self.channel_name
        )
        # WebSocket 연결
        self.accept()

    def disconnect(self, close_code):
        async_to_sync(self.channel_layer.group_discard)(
            self.room_group_name,
            self.channel_name
        )
        self.close()

    # lambda handler

    def handle(event, context):
        layers = channels.layers.get_channel_layer()
        data = event['Records'][0]
        eventName = data['eventName']
        today = datetime.date.today().isoformat()
        status_1_count = 0
        status_2_count = 0
        send_data = ''
        status = ''
        diffkeys = []

        # 상태코드 변환
        def statusCode():
            if data['dynamodb']['NewImage']['status']['S'] == '1':
                status = '판매중'
                return status

            elif data['dynamodb']['NewImage']['status']['S'] == '2':
                status = "대기"
                return status

        # 총갯수, 상태에 따른 갯수 통계
        today_count = table.scan(
            FilterExpression=Key('created_at').begins_with(today))

        for el in today_count['Items']:
            if el["status"] == '1':
                status_1_count += 1
            elif el["status"] == '2':
                status_2_count += 1

        # eventName에 따른 분기
        if (eventName == "INSERT") or (eventName == "MODIFY"):

            # 전에 있던 데이터와 고친 데이터의 차이점을 찾아서 보낸다.
            if eventName == 'MODIFY':
                diffresult = ''

                newdict = list(data['dynamodb']['NewImage'].items())
                olddict = list(data['dynamodb']['OldImage'].items())

                for i in range(0, len(newdict)):
                    if newdict[i] != olddict[i]:
                        diffresult = newdict[i]
                        diffkeys.append(diffresult[0])

            send_data = {
                'type': eventName,
                'product_id': data['dynamodb']['NewImage']['product_id']['N'],
                'product': data['dynamodb']['NewImage']['product']['S'],
                'quantity': data['dynamodb']['NewImage']['quantity']['N'],
                'price': data['dynamodb']['NewImage']['price']['N'],
                'store_name': data['dynamodb']['NewImage']['store_name']['S'],
                'store_lng': data['dynamodb']['NewImage']['longitude']['S'],
                'store_lat': data['dynamodb']['NewImage']['latitude']['S'],
                'status': statusCode(),
                'created_at': data['dynamodb']['NewImage']['created_at']['S'],
                'diffKeys': diffkeys,
                'count': {
                    'today_count': today_count['Count'],
                    'status_1_count': status_1_count,
                    'status_2_count': status_2_count
                }
            }

        elif eventName == 'REMOVE':
            send_data = {
                'type': eventName,
                'product_id': data['dynamodb']['OldImage']['product_id']
            }

        async_to_sync(layers.group_send)('product_product', {
            'type': 'order_message',
            'data': send_data,
        })
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from lambda')
        }

    def order_message(self, event):
        message = event
        channel_layers = channels.layers.get_channel_layer()
        self.send(text_data=json.dumps(message))
