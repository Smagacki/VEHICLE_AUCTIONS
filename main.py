import asyncio
import csv
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from enum import Enum
from pathlib import Path
from typing import Any, Optional
from concurrent.futures import ThreadPoolExecutor
from pydantic.dataclasses import dataclass
from pydantic import Field


# MODELE DANYCH

class Transmission(Enum):
    AUTOMATIC = "Automatic"
    MANUAL = "Manual"
    UNKNOWN = "Unknown"

    @classmethod
    def from_str(cls, value: str) -> "Transmission":
        clean_val = str(value).strip() if value else ""

        for member in cls:
            if member.value.lower() == clean_val.lower():
                return member

        return cls.UNKNOWN


@dataclass
class Engine:
    description: str
    cylinders: Optional[str]


@dataclass
class Vehicle:
    year: int
    make: str
    model: str
    vin: str
    engine: Engine
    transmission: Transmission


@dataclass
class Auction:
    date_utc: datetime
    location: str
    vehicles: list[Vehicle] = Field(default_factory=list)

    def display_local_time(self, tz_name: str = "Europe/Warsaw") -> str:
        local_tz = ZoneInfo(tz_name)
        return self.date_utc.astimezone(local_tz).strftime("%Y-%m-%d %H:%M %Z")


# LOGIKA PRZETWARZANIA

class DateParser:
    TIMEZONE_MAP = {
        "CST": "America/Chicago", "CDT": "America/Chicago",
        "EST": "America/New_York", "EDT": "America/New_York",
        "PST": "America/Los_Angeles", "PDT": "America/Los_Angeles",
        "MST": "America/Denver", "MDT": "America/Denver",
    }

    @classmethod
    def parse(cls, date_str: str, year: int) -> datetime:
        pattern = r'(\w{3} \w{3} \d{2}, \d{1,2}:\d{2}\s*(?:am|pm))\s*([A-Z]{3})?'
        match = re.search(pattern, date_str.strip(), re.IGNORECASE)
        if not match:
            raise ValueError(f"Nieprawidłowy format daty: {date_str}")

        date_part = " ".join(f"{year} {match.group(1).upper()}".split())
        tz_abbrev = match.group(2).upper() if match.group(2) else "UTC"

        iana_name = cls.TIMEZONE_MAP.get(tz_abbrev, "UTC")
        naive_date = datetime.strptime(date_part, "%Y %a %b %d, %I:%M%p")
        return naive_date.replace(tzinfo=ZoneInfo(iana_name)).astimezone(timezone.utc)


class VehicleFactory:
    @staticmethod
    def create_from_csv_row(row: dict[str, Any]) -> Vehicle:
        engine = Engine(
            description=row.get('Engine', 'N/A'),
            cylinders=row.get('Cylinders', 'N/A')
        )

        return Vehicle(
            year=row.get('Year'),
            make=row.get('Make', 'Unknown'),
            model=row.get('Model', 'Unknown'),
            vin=row.get('Vin#', 'Unknown'),
            engine=engine,
            transmission=Transmission.from_str(row.get('Transmission Type', ''))
        )


class DataProcessor:
    @staticmethod
    def extract_year_from_filename(file_name: str) -> int:
        match = re.search(r'(20\d{2}|19\d{2})', file_name)
        if match: return int(match.group(1))
        raise ValueError(f"Brak roku w nazwie pliku: {file_name}")

    def process_file(self, file_path: Path) -> list[tuple[datetime, str, Vehicle]]:
        results = []
        try:
            file_year = self.extract_year_from_filename(file_path.name)

            with open(file_path, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        vehicle = VehicleFactory.create_from_csv_row(row)
                        date_utc = DateParser.parse(row['Auction Date'], file_year)
                        results.append((date_utc, row['Branch Name'], vehicle))
                    except (ValueError, KeyError, TypeError) as e:
                        print(f"Błąd wiersza w {file_path.name}: {e}")
                        continue

        except Exception as e:
            print(f"Błąd pliku {file_path.name}: {e}")
            return []

        return results


# PRZETWARZANIE RÓWNOLEGŁE

async def run_pipeline(directory_name: str) -> list[Auction]:
    path = Path(directory_name)
    if not path.exists():
        raise FileNotFoundError(f"Katalog '{directory_name}' nie istnieje!")

    files = list(path.glob("*.csv"))
    processor = DataProcessor()
    loop = asyncio.get_running_loop()

    with ThreadPoolExecutor() as pool:
        tasks = [loop.run_in_executor(pool, processor.process_file, f) for f in files]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    auctions: dict[tuple[datetime, str], Auction] = {}

    for file_result in results:
        if isinstance(file_result, Exception):
            print(f"Zadanie zwróciło nieoczekiwany wyjątek: {file_result}")
            continue

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
