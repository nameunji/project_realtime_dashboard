import order.routing
import product.routing

from channels.auth    import AuthMiddlewareStack
from channels.routing import ProtocolTypeRouter, URLRouter

application = ProtocolTypeRouter({
    'websocket': AuthMiddlewareStack(
        URLRouter(
            [
                order.routing.websocket_urlpatterns,
                product.routing.websocket_urlpatterns
            ]
        )
    )
})