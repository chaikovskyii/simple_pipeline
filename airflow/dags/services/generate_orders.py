import uuid
import random
import psycopg2
from datetime import datetime, timedelta
from faker import Faker
from typing import List, Dict, Any
from config import ETLConfig


class OrderGenerator:
    
    DEFAULT_ORDER_COUNT = ETLConfig.ORDERS_BATCH_SIZE
    MIN_AMOUNT = 10.0
    MAX_AMOUNT = 1000.0
    
    def __init__(self):
        self.fake = Faker()
        self.currencies = self._get_currencies_from_api()
    
    def _get_currencies_from_api(self) -> List[str]:
        from .currency_service import currency_service
        service = currency_service
        
        all_currencies = service.get_supported_currencies()
        validated_currencies = [
            curr for curr in all_currencies 
            if self._is_valid_currency(service, curr)
        ]
        
        return validated_currencies or ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD']
    
    def _is_valid_currency(self, service, currency: str) -> bool:
        try:
            result = service.convert_to_eur(100.0, currency)
            return result and result.get('eur_amount', 0) > 0
        except (ValueError, TypeError) as e:
            return False
        except Exception as e:
            return False
    
    def generate_orders(self, count: int = None) -> List[Dict[str, Any]]:
        count = count or self.DEFAULT_ORDER_COUNT
        orders = []
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        for _ in range(count):
            order = {
                'order_id': str(uuid.uuid4()),
                'customer_email': self.fake.email(),
                'order_date': self.fake.date_time_between(start_date=start_date, end_date=end_date),
                'amount': round(random.uniform(self.MIN_AMOUNT, self.MAX_AMOUNT), 2),
                'currency': random.choice(self.currencies)
            }
            orders.append(order)
        
        return orders
    


def generate_and_insert_orders():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2.extras import execute_values
    
    generator = OrderGenerator()
    orders = generator.generate_orders()
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_1')
    
    insert_sql = """
        INSERT INTO orders (order_id, customer_email, order_date, amount, currency)
        VALUES %s
    """
    
    insert_data = []
    for order in orders:
        insert_data.append((
            order['order_id'],
            order['customer_email'], 
            order['order_date'],
            order['amount'],
            order['currency']
        ))
    
    conn = None
    cursor = None
    try:
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        execute_values(cursor, insert_sql, insert_data)
        conn.commit()
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()