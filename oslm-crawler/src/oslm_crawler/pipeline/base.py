from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Generator, TypeAlias


@dataclass
class PipelineData:
    
    data: dict | None
    message: dict | None
    error: dict | None
    
    
PipelineResult: TypeAlias = Generator[PipelineData, None, None] | None


class PipelineStep(ABC):
    
    ptype: str = ""
    required_keys = []
    
    def __init__(self):
        super().__init__()
        
    @abstractmethod
    def parse_input(self, input_data: PipelineData | None = None):
        self.input = None
        
    @abstractmethod
    def run(self) -> PipelineResult:
        pass
    