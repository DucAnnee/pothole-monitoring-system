from abc import ABC, abstractmethod
from dataclasses import dataclass
import random


@dataclass
class GpsReading:
    lat: float
    lon: float
    accuracy_m: float | None = None


class GpsProvider(ABC):
    @abstractmethod
    def read(self) -> GpsReading:
        ...


class SimulatedGpsProvider(GpsProvider):
    def __init__(
        self,
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        accuracy_min_m: float = 5.0,
        accuracy_max_m: float = 15.0,
    ):
        self._lat_min = lat_min
        self._lat_max = lat_max
        self._lon_min = lon_min
        self._lon_max = lon_max
        self._accuracy_min_m = accuracy_min_m
        self._accuracy_max_m = accuracy_max_m

    def read(self) -> GpsReading:
        return GpsReading(
            lat=random.uniform(self._lat_min, self._lat_max),
            lon=random.uniform(self._lon_min, self._lon_max),
            accuracy_m=random.uniform(self._accuracy_min_m, self._accuracy_max_m),
        )
