import os
import django
import json
import logging
from kafka import KafkaConsumer

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'order_system.settings')
django.setup()


from orders_app.models import Order

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

consumer = KafkaConsumer(
    'order_created',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='order_consumers'
)



def process_order(order_data):
    order_id = order_data.get("order_id")
    logging.info(f"Processing order {order_id}")
    # product_name = order_date.get("product_name")

    try:
        order = Order.objects.get(id=order_id)
        if order.quantity < 10:
            order.status = "PROCESSED"
        else:
            order.status = "REJECTED"
        
        order.save()
        logger.info(f"Processed order {order_id}")
    except Order.DoesNotExist:
        logger.error(f"Order {order_id} does not exist: {order_id}", )

for message in consumer:
    order_data = message.value
    logger.info(f"Received order event: {order_data}")
    try:
        process_order(order_data)
        consumer.commit()
    except Exception as e:
        logger.error(f"Error processing order event: {e}")  