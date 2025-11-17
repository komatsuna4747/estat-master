from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class BaseETLConfig:
    output_path: str
    revision: str
    output_format: str = "json"
    debug_code_limit: int | None = None


class BaseETL[TIn, TOut](ABC):
    @abstractmethod
    def extract(self) -> TIn:
        pass

    @abstractmethod
    def transform(self, data: TIn) -> TOut:
        pass

    @abstractmethod
    def load(self, data: TOut) -> None:
        pass

    def run(self) -> None:
        data = self.extract()
        transformed_data = self.transform(data)
        self.load(transformed_data)
