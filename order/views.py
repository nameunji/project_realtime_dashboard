import json
import websocket
import real_settings

from django.views                 import View

from django.views.decorators.csrf import csrf_exempt
from django.http                  import HttpResponse
from websocket                    import create_connection


@csrf_exempt
def lambdaClient(request):
    request_data = json.loads(request.body)
    if len(request_data) > 0:
        json_data = json.dumps(request_data)
        ws = create_connection("ws://" + real_settings.WEBSOCKET_SERVER + "/ws/order/")
        ws.send(json_data)
        ws.close()
    return HttpResponse(status = 200)
