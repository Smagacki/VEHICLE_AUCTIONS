import unittest
import pytz
from main import DataProcessor, Vehicle, Engine, Transmission


class TestAuctionSystem(unittest.TestCase):

    def test_timezone_conversion(self):
        # Czy czas CST (UTC-6) jest poprawnie przeliczany na UTC
        date_str = "Mon Dec 01, 8:30am CST"
        res = DataProcessor.parse_auction_date(date_str)

        # 8:30 rano w CST to 14:30 w UTC
        self.assertEqual(res.hour, 14)
        self.assertEqual(res.minute, 30)
        self.assertEqual(res.tzinfo, pytz.UTC)

    def test_pydantic_validation_error(self):
        # Czy Pydantic zablokuje błędny typ danych."""
        with self.assertRaises(Exception):
            Vehicle(
                year="STARY",
                make="Audi",
                model="A4",
                vin="123",
                engine=Engine("V6", "6"),
                transmission=Transmission.AUTOMATIC
            )

    def test_transmission_enum(self):
        # Czy nieznana skrzynia biegów ustawia status UNKNOWN."""
        v = Vehicle(
            year=2020, make="Test", model="X", vin="000",
            engine=Engine("2.0", "4"),
            transmission=Transmission("Unknown")
        )
        self.assertEqual(v.transmission, Transmission.UNKNOWN)


if __name__ == "__main__":
    unittest.main()