import pytest
from datetime import datetime, timezone
from main import DateParser, Vehicle, Engine, Transmission, VehicleFactory


@pytest.mark.parametrize("input_value, expected_enum", [
    ("Automatic", Transmission.AUTOMATIC),
    ("Manual", Transmission.MANUAL),
    ("  automatic  ", Transmission.AUTOMATIC),
    ("Electric", Transmission.UNKNOWN),
    ("", Transmission.UNKNOWN),
    (None, Transmission.UNKNOWN),
])
def test_transmission_conversion(input_value, expected_enum):
    assert Transmission.from_str(input_value) == expected_enum


def test_date_parsing_cst_to_utc():
    date_str = "Mon Dec 01, 8:30am CST"
    year = 2025
    res = DateParser.parse(date_str, year)

    assert res.hour == 14
    assert res.minute == 30
    assert res.tzinfo == timezone.utc


def test_pydantic_type_coercion():
    v = Vehicle(
        year="2020",
        make="Audi",
        model="A4",
        vin="123",
        engine=Engine("V6", "6"),
        transmission=Transmission.AUTOMATIC
    )
    assert v.year == 2020
    assert isinstance(v.year, int)


def test_pydantic_validation_error():
    with pytest.raises(Exception):
        Vehicle(
            year="STARY_ROK",
            make="Audi",
            model="A4",
            vin="123",
            engine=Engine("V6", "6"),
            transmission=Transmission.AUTOMATIC
        )


def test_vehicle_factory_missing_data():
    row = {
        "Year": "2015",
        "Make": "Toyota",
        "Model": "Corolla",
        "Vin#": "VIN123"
    }
    vehicle = VehicleFactory.create_from_csv_row(row)

    assert vehicle.year == 2015
    assert vehicle.engine.description == "N/A"
    assert vehicle.transmission == Transmission.UNKNOWN
