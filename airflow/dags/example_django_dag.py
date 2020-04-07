from airflow import DAG
from airflow.models import BaseOperator
from datetime import datetime
import logging
import os
import sys

class DjangoOperator(BaseOperator):

    def pre_execute(self, context):
        logging.info('Pre-execute hook')
        sys.path.append('/usr/local')
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'pizza.settings')

        logging.info('os.path.dirname(os.path.realpath(__file__))')
        logging.info(os.path.dirname(os.path.realpath(__file__)))
        logging.info('------------------------------------------')

        logging.info('sys.path')
        logging.info(sys.path)

        import django
        django.setup()

class DjangoExampleOperator(DjangoOperator):

    def execute(self, context):
        logging.info('Main method')
        from order.models import Order
        logging.info('Get Order objects')
        logging.info(Order.objects.get_or_create())
        return Order.objects.get_or_create()

default_args = {
        "description": "Test Django Operator",
        "start_date": datetime(2020, 4, 7),
        "catchup": False
}

dag = DAG(
        "get_list_of_orders",
        default_args=default_args,
        schedule_interval=None
)

t1 = DjangoExampleOperator(
        task_id="list_order",
        dag=dag)

t1