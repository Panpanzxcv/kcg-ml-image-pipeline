from datetime import datetime, timezone
import secrets
from typing import List

class Uuid64():
    _value: int = 0;

    def __init__(self):
        # Create with the current UTC date.
        self._create_value_with_date(datetime.now(timezone.utc))
    
    def __str__(self):
        hex_string = self._value.to_bytes(8, 'big').hex()
        return (hex_string[0:4] + '-' + hex_string[4:8] + '-' + hex_string[8:12] + '-' + hex_string[12:16]).upper()
    
    @staticmethod
    def from_date_string(date: str, date_formats: List[str]):
        if not date_formats:
            raise ValueError(f"date_formats must include at least one format")

        for fmt in date_formats:
            # Try to create a date from the string.
            try:
                date_value = datetime.strptime(date, fmt)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"time data '{date}' does not match any of the provided formats")
        
        instance = Uuid64();
        instance._create_value_with_date(date_value)

        return instance
    
    @staticmethod
    def from_mongo_value(value: int):
        if value < 0 or value > 18446744073709551615:
            raise ValueError(f"the value is not a valid uuid")
        
        instance = Uuid64();
        instance._value = value

        return instance
    
    def to_mongo_value(self):
        return self.value
    
    def to_formatted_str(self):
        return str(self)

    def _create_value_with_date(self, date_value: datetime):
        # Posix time as a 32bit unsigned int
        unix_time_32bit = int(date_value.timestamp()) & 0xFFFFFFFF
        # Secure 32bit random number as 32 unsigned int
        random_32bit = int(secrets.randbits(32)) & 0xFFFFFFFF
        # 64bit number. Date in the first 32 bits and the random number in the last 32 bits
        self._value = (random_32bit & 0xFFFFFFFF) | (unix_time_32bit << 32)
