from typing import Dict, List, Any
from datetime import datetime
from contextlib import contextmanager
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
from .currency_service import currency_service
from config import DatabaseConfig, ETLConfig


class OrdersETL:
    
    DEFAULT_BATCH_SIZE = DatabaseConfig.DEFAULT_BATCH_SIZE
    DEFAULT_MAX_RUNTIME_MINUTES = ETLConfig.MAX_RUNTIME_MINUTES
    
    def __init__(self):
        from .currency_service import currency_service
        self.currency_service = currency_service
        self.source_hook = PostgresHook(postgres_conn_id='postgres_1')
        self.target_hook = PostgresHook(postgres_conn_id='postgres_2')
    
    @contextmanager
    def _get_connection(self, hook):
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        except (psycopg2.Error, Exception) as e:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    
    def get_unprocessed_orders(self, batch_size: int = None) -> List[tuple]:
        batch_size = batch_size or DatabaseConfig.DEFAULT_BATCH_SIZE
        try:
            with self._get_connection(self.source_hook) as cursor:
                cursor.execute("""
                    SELECT order_id, customer_email, order_date, amount, currency
                    FROM orders 
                    WHERE processed_at IS NULL
                    ORDER BY created_at
                    LIMIT %s
                """, (batch_size,))
                return cursor.fetchall()
        except (psycopg2.Error, psycopg2.OperationalError) as e:
            return []
        except Exception as e:
            return []
    
    def process_and_save_orders(self, orders: List[tuple]) -> Dict[str, Any]:
        if not orders:
            return {
                'processed': 0,
                'errors': 0,
                'status': 'success',
                'message': 'No orders to process'
            }
        
        processed_count = 0
        error_count = 0
        successful_order_ids = []
        
        with self._get_connection(self.target_hook) as cursor:
            for order in orders:
                try:
                    order_id, customer_email, order_date, amount, currency = order
                    
                    conversion_result = self.currency_service.convert_to_eur(float(amount), currency)
                    
                    cursor.execute("""
                        INSERT INTO orders_eur (
                            order_id, customer_email, order_date, amount, currency
                        ) VALUES (%s, %s, %s, %s, 'EUR')
                    """, (order_id, customer_email, order_date, conversion_result['eur_amount']))
                    
                    successful_order_ids.append(order_id)
                    processed_count += 1
                except (ValueError, TypeError) as e:
                    error_count += 1
                    continue
                except psycopg2.Error as e:
                    error_count += 1
                    continue
                except Exception as e:
                    error_count += 1
                    continue
        
        if successful_order_ids:
            self._mark_orders_processed(successful_order_ids)
        
        return {
            'processed': processed_count,
            'errors': error_count,
            'status': 'success',
            'message': f'Processed {processed_count} orders, {error_count} errors'
        }
    
    def _mark_orders_processed(self, order_ids: List[str]) -> None:
        if not order_ids:
            return
        
        try:
            with self._get_connection(self.source_hook) as cursor:
                placeholders = ','.join(['%s'] * len(order_ids))
                cursor.execute(f"""
                    UPDATE orders 
                    SET processed_at = CURRENT_TIMESTAMP 
                    WHERE order_id IN ({placeholders})
                """, order_ids)
        except psycopg2.Error as e:
            pass
        except Exception as e:
            pass
    
    def run_etl(self, batch_size: int = DEFAULT_BATCH_SIZE, max_runtime_minutes: int = DEFAULT_MAX_RUNTIME_MINUTES) -> Dict[str, Any]:
        start_time = datetime.now()
        total_processed = 0
        total_errors = 0
        batch_count = 0
        
        while True:
            elapsed_minutes = (datetime.now() - start_time).total_seconds() / 60
            if elapsed_minutes > max_runtime_minutes:
                break
            
            orders = self.get_unprocessed_orders(batch_size)
            
            if not orders:
                break
            
            batch_count += 1
            result = self.process_and_save_orders(orders)
            total_processed += result['processed']
            total_errors += result['errors']
            
            if result['processed'] == 0:
                break
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        status = 'success' if total_errors == 0 else ('partial_success' if total_processed > 0 else 'error')
        
        return {
            'status': status,
            'orders_processed': total_processed,
            'total_errors': total_errors,
            'batches_processed': batch_count,
            'processing_time_seconds': processing_time,
            'message': f'Processed {total_processed} orders in {batch_count} batches with {total_errors} errors'
        }


def run_currency_etl():
    etl = OrdersETL()
    result = etl.run_etl()
    
    if result['status'] == 'error':
        raise Exception(f"ETL pipeline failed: {result['message']}")
    
    return result