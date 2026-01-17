import asyncio
import csv
import pytz
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
from pydantic.dataclasses import dataclass as pydantic_dataclass


# MODELE DANYCH

class Transmission(Enum):
    AUTOMATIC = "Automatic"
    MANUAL = "Manual"
    UNKNOWN = "Unknown"

    @classmethod
    def from_str(cls, value: str):
        clean_val = str(value).strip() if value else ""

        if not clean_val:
            return cls.UNKNOWN

        for member in cls:
            if member.value.lower() == clean_val.lower():
                return member

        return cls.UNKNOWN


@pydantic_dataclass
class Engine:
    description: str
    cylinders: Optional[str]


@pydantic_dataclass
class Vehicle:
    year: int
    make: str
    model: str
    vin: str
    engine: Engine
    transmission: Transmission


@dataclass(frozen=True)
class Auction:
    date_utc: datetime
    location: str
    vehicles: List[Vehicle] = field(default_factory=list, hash=False, compare=False)

    def display_local_time(self, tz_name: str = "Europe/Warsaw") -> str:
        local_tz = pytz.timezone(tz_name)
        return self.date_utc.astimezone(local_tz).strftime("%Y-%m-%d %H:%M %Z")


# LOGIKA PRZETWARZANIA

class DataProcessor:
    @staticmethod
    def parse_auction_date(date_str: str) -> datetime:
        clean_date = date_str.replace("am", "AM").replace("pm", "PM")

        match = re.search(r'[a-zA-Z]{3} [a-zA-Z]{3} \d{2}, \d{1,2}:\d{2}[APM]{2}', clean_date)
        if match:
            clean_date = match.group(0)

        current_year = datetime.now().year
        date_with_year = f"{current_year} {clean_date}"

        naive_date = datetime.strptime(date_with_year, "%Y %a %b %d, %I:%M%p")

        central_tz = pytz.timezone("US/Central")
        localized = central_tz.localize(naive_date)

        return localized.astimezone(pytz.UTC)

    @classmethod
    def process_file(cls, file_path: Path) -> List[Tuple[datetime, str, Vehicle]]:
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return [
                (
                    cls.parse_auction_date(row['Auction Date']),
                    row['Branch Name'],
                    Vehicle(
                        year=int(row['Year']),
                        make=row['Make'],
                        model=row['Model'],
                        vin=row['Vin#'],
                        engine=Engine(row['Engine'], row.get('Cylinders', 'N/A')),
                        transmission=Transmission.from_str(row.get('Transmission Type', ''))
                    )
                )
                for row in reader
            ]


# PRZETWARZANIE RÓWNOLEGŁE

async def run_pipeline(directory_name: str):
    path = Path(directory_name)
    if not path.exists():
        raise FileNotFoundError(f"Katalog '{directory_name}' nie istnieje!")

    files = list(path.glob("*.csv"))
    loop = asyncio.get_running_loop()

    with ThreadPoolExecutor() as pool:
        tasks = [loop.run_in_executor(pool, DataProcessor.process_file, f) for f in files]
        results = await asyncio.gather(*tasks)

    auctions: Dict[Tuple[datetime, str], Auction] = {}

    for file_result in results:
        for date_utc, loc, vehicle in file_result:
            key = (date_utc, loc)
            if key not in auctions:
                auctions[key] = Auction(date_utc=date_utc, location=loc)
            auctions[key].vehicles.append(vehicle)

    return list(auctions.values())


if __name__ == "__main__":
    FOLDER_NAME = "csv_files"

    try:
        all_auctions = asyncio.run(run_pipeline(FOLDER_NAME))

        print(f"--- RAPORT Z PRZETWARZANIA ({FOLDER_NAME}) ---\n")
        for auc in all_auctions:
            print(f"Lokalizacja: {auc.location}")
            print(f"Data (Lokalna PL): {auc.display_local_time()}")
            print(f"Liczba pojazdów: {len(auc.vehicles)}")
            if auc.vehicles:
                v = auc.vehicles[0]
                print(f"Przykładowy pojazd: {v.year} {v.make} {v.model} [VIN: {v.vin}]")
            print("-" * 40)

    except FileNotFoundError as e:
        print(f"Błąd: {e}. Upewnij się, że folder znajduje się w katalogu z projektem.")
    except Exception as e:
        print(f"Wystąpił nieoczekiwany błąd: {e}")
