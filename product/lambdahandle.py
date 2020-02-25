import json
from .consumers import ProductConsumer


def invoke_hander(event, context):
    return ProductConsumer.handle(event, context)
