from rest_framework.decorators import api_view
from rest_framework.response import Response
from .models import Order
from .serializers import OrderSerializer
from .kafka_producer import publish_order_event

@api_view(['POST'])
def create_order(request):
    print("Received order creation request:", request.data)
    serializer = OrderSerializer(data=request.data)

    if serializer.is_valid():
        order = serializer.save()

        publish_order_event({
            "order_id": str(order.id),
            "product_name": order.product_name,
            "quantity": order.quantity,
        })

        return Response(serializer.data)

    return Response(serializer.errors, status=400)