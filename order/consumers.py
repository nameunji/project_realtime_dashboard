import json
import boto3
import decimal

from datetime import date
from boto3.dynamodb.conditions import Key, Attr
from asgiref.sync import async_to_sync
from django.core.serializers.json import DjangoJSONEncoder
from channels.generic.websocket import AsyncWebsocketConsumer

# decimal형을 int/float형으로 변경해주는 class(json때문에) 
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class OrderConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.order_name = 'order'
        self.order_group_name = 'order_%s' % self.order_name

        # join group
        await self.channel_layer.group_add(
            self.order_group_name,
            self.channel_name
        )
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.order_group_name,
            self.channel_name
        )

    # receive message from websocket
    async def receive(self, text_data):

        # 당일 데이터 전체 스캔 작업
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('realtime_order')

        today = date.today().isoformat()
        
        today_all_data = table.scan(FilterExpression=Key(
            'updated_at').begins_with(today))["Items"]

        ## status 통계 & today list(map)
        order_count = 0
        receipt_count = 0

        ## 추후 맵에 status에 따른 마커 이미지 변경을 위해 남겨둠
        todaylist_before = {
            "201": [],
            "210": []
        }

        for el in today_all_data:
            if el["status"] == "201":
                order_count += 1
                todaylist_before["201"].append(el)
            elif el["status"] == "210":
                receipt_count += 1
                todaylist_before["210"].append(el)

        orderStatus = {
            "order": order_count,
            "receipt": receipt_count
        }
        ## 추후 맵에 status에 따른 마커 이미지 변경을 위해 남겨둠
        ### todayList = json.loads(json.dumps(todaylist_before, cls=DecimalEncoder))
        todaylist = json.loads(json.dumps(today_all_data, cls=DecimalEncoder))

        # 람다 데이터
        data = json.loads(text_data)

        message = {
            "data": data,
            "orderStatus": orderStatus,
            "todaydata": todaylist
        }
        # send message to group
        await self.channel_layer.group_send(
            self.order_group_name,
            {
                'type': 'order_message',
                'message': message
            }
        )

    # receive message from room group
    async def order_message(self, event):
        message = event['message']

        await self.send(text_data=json.dumps({
            'message': message
        }))
