import json
import boto3
import datetime
from django.http import JsonResponse
from django.views import View
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('realtime_product')

now = datetime.date.today()


class Product(View):
    def get(self, request):
        status_1_count = 0
        status_2_count = 0

        products = table.scan(
            FilterExpression=Attr('created_at').begins_with(str(now))
        )

        location = []

        for el in products['Items']:
            if el["status"] == '1':
                status_1_count += 1
            elif el["status"] == '2':
                status_2_count += 1
            location.append({
                'lng': el['longitude'],
                'lat': el['latitude']
            })

        product_list = sorted(products['Items'], key=lambda x: datetime.datetime.strptime(
            x['created_at'], '%Y-%m-%d %H:%M:%S.%f'), reverse=True)

        return JsonResponse(
            {
                'data': product_list,
                'count':
                {
                    'today_count': products['Count'],
                    'status_1_count': status_1_count,
                    'status_2_count': status_2_count
                },
                'location': location
            }
        )
