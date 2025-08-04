import requests
from typing import Dict
from datetime import datetime
import os
from config import APIConfig, ETLConfig


class CurrencyService:
    
    def __init__(self):
        self.api_key = os.getenv('OPENEXCHANGE_API_KEY')
        if not self.api_key:
            raise ValueError("OPENEXCHANGE_API_KEY environment variable is required")
        self.base_url = 'https://openexchangerates.org/api'
        self._rates_cache = None
        self._cache_timestamp = None
        self._cache_duration = APIConfig.CACHE_DURATION
    
    def get_latest_rates(self) -> Dict[str, float]:
        if self._is_cache_valid():
            return self._rates_cache
        
        url = f"{self.base_url}/latest.json"
        params = {
            'app_id': self.api_key,
            'base': 'USD'
        }
        
        try:
            response = requests.get(url, params=params, timeout=APIConfig.REQUEST_TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            
            if not isinstance(data, dict):
                raise ValueError("API response is not a valid JSON object")
            
            if 'rates' not in data:
                raise ValueError("API response missing 'rates' field")
            
            rates = data['rates']
            if not isinstance(rates, dict) or not rates:
                raise ValueError("API response 'rates' field is not a valid dictionary or is empty")
            
            required_currencies = ['EUR', 'USD']
            missing_currencies = [curr for curr in required_currencies if curr not in rates]
            if missing_currencies:
                raise ValueError(f"API response missing required currencies: {missing_currencies}")
            
            self._rates_cache = rates
            self._cache_timestamp = datetime.now()
            return rates
            
        except requests.exceptions.RequestException as e:
            raise
        except (ValueError, KeyError) as e:
            raise
    
    def get_eur_rate(self, from_currency: str) -> float:
        if from_currency == 'EUR':
            return 1.0
        
        rates = self.get_latest_rates()
        
        from_currency_rate = rates.get(from_currency)
        if from_currency_rate is None:
            raise ValueError(f"Currency {from_currency} not found in rates")
        
        eur_rate = rates.get('EUR')
        if eur_rate is None:
            raise ValueError("EUR not found in rates")
        
        currency_to_usd = 1 / from_currency_rate
        if currency_to_usd < ETLConfig.MIN_CURRENCY_RATE:
            raise ValueError(f"Unsupported currency rate for {from_currency}: {currency_to_usd}")
        
        rate = currency_to_usd * eur_rate
        
        if rate <= 0:
            raise ValueError(f"Invalid rate calculated for {from_currency}: {rate}")
        
        return rate
    
    def convert_to_eur(self, amount: float, from_currency: str) -> Dict[str, any]:
        rate = self.get_eur_rate(from_currency)
        eur_amount = amount * rate
        
        if eur_amount <= 0:
            raise ValueError(f"Conversion resulted in zero or negative amount: {eur_amount} EUR from {amount} {from_currency}")
        
        return {
            'original_amount': amount,
            'original_currency': from_currency,
            'eur_amount': round(eur_amount, 2),
            'exchange_rate': rate,
            'converted_at': datetime.now().isoformat()
        }
    
    def get_supported_currencies(self) -> list:
        rates = self.get_latest_rates()
        return [currency for currency, rate in rates.items() if rate >= ETLConfig.MIN_CURRENCY_RATE]
    
    def _is_cache_valid(self) -> bool:
        return (self._rates_cache is not None and 
                self._cache_timestamp is not None and 
                (datetime.now() - self._cache_timestamp).total_seconds() < self._cache_duration)


currency_service = CurrencyService()