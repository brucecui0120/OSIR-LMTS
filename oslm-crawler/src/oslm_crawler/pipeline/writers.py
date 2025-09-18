import jsonlines
import traceback
from .base import PipelineStep, PipelineResult, PipelineData
from pathlib import Path
from loguru import logger


class JsonlineWriter(PipelineStep):
    
    ptype = "✍️ WRITER"
    required_keys = []
    
    def __init__(
        self,
        path: str,
        required_keys: list[str] | None = None,
        drop_keys: list[str] | None = None,
    ):
        self.required_keys = required_keys
        if drop_keys:
            self.drop_keys = drop_keys
        else:
            self.drop_keys = []
        self.path = Path(path)
        assert self.path.suffix == '.jsonl', 'The path must end with a filename that has a `.jsonl` suffix.'
        self.path.parent.mkdir(exist_ok=True)
        self.path.touch()
        self.f = open(self.path, 'w')
        self.writer = jsonlines.Writer(self.f)
    
    def parse_input(self, input_data: PipelineData | None = None):
        if self.required_keys is None:
            self.required_keys = list(input_data.data.keys())
        self.required_keys = [x for x in self.required_keys if x not in self.drop_keys]
        self.data = input_data.data.copy()
        required_data = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            required_data[k] = self.data[k]
        self.input = required_data
        
    def run(self) -> PipelineResult:
        try:
            self.writer.write(self.input)
            self.f.flush()
            yield PipelineData(self.data, None, None)
        except Exception:
            logger.exception(f"Error write jsonline data:\n {self.input}")
            yield PipelineData(None, None, {
                'input': self.input,
                'error_msg': traceback.format_exc(),
            })
        
    def close(self) -> bool:
        try:
            self.writer.close()
            self.f.close()
            return True
        except Exception:
            logger.exception(f'Error when close JsonlineWriter with path={self.path}')
            return False
        
        
class ModelDatasetJsonlineWriter(PipelineStep):
    
    ptype = "✍️ WRITER"
    
    def __init__(
        self,
        model_path: str,
        dataset_path: str,
        model_drop_keys: list[str] | None = None,
        dataset_drop_keys: list[str] | None = None,
    ):
        self.model_writer = JsonlineWriter(model_path, drop_keys=model_drop_keys)
        self.dataset_writer = JsonlineWriter(dataset_path, drop_keys=dataset_drop_keys)
    
    def parse_input(self, input_data: PipelineData | None = None):
        if 'model_name' in input_data.data or input_data.data.get('category', None) == 'models':
            self.next_write = "models"
            self.model_writer.parse_input(input_data)
        elif 'dataset_name' in input_data.data or input_data.get('category', None) == 'datasets':
            self.next_write = "datasets"
            self.dataset_writer.parse_input(input_data)
        else:
            raise RuntimeError(f"'model_name' or 'dataset_name' not found in {input_data.data.keys()}")
    
    def run(self) -> PipelineResult:
        match self.next_write:
            case "models":
                yield next(self.model_writer.run())
            case "datasets":
                yield next(self.dataset_writer.run())
                
    def close(self) -> bool:
        self.model_writer.close()
        self.dataset_writer.close()

        
class ListWriter(PipelineStep):
    
    ptype = "✍️ WRITER"
    required_keys = []
    
    def __init__(
        self,
        required_keys: list[str] | None = None,
        drop_keys: list[str] | None = None,
    ):
        self.required_keys = required_keys
        if drop_keys:
            self.drop_keys = drop_keys
        else:
            self.drop_keys = []
        self.collector = []
        
    def parse_input(self, input_data: PipelineData | None = None):
        if self.required_keys is None:
            self.required_keys = list(input_data.data.keys())
        self.required_keys = [x for x in self.required_keys if x not in self.drop_keys]
        self.data = input_data.data.copy()
        required_data = {}
        for k in self.required_keys:
            if k not in self.data:
                raise KeyError(f"key '{k}' not found in input_data.data "
                               f"{list(input_data.data.keys())} of {self.__class__}")
            required_data[k] = self.data[k]
        self.input = required_data
        
    def run(self) -> PipelineResult:
        try:
            self.collector.append(self.input)
            yield PipelineData({"content": self.collector}, {"length": len(self.collector)}, None)
        except Exception:
            logger.exception(f"Error write jsonline data:\n {self.input}")
            yield PipelineData(None, None, {
                'input': self.input,
                'error_msg': traceback.format_exc(),
            })
    
    def close(self) -> bool:
        self.collector.clear()
        return True
    

# TODO database writer    
class DBWriter(PipelineStep):
    
    ptype = "✍️ WRITER"
    
    def __init__(
        self,
        conn,
    ):
        raise NotImplementedError
    
    def parse_input(self, input_data: PipelineData | None = None):
        raise NotImplementedError
    
    def run(self) -> PipelineResult:
        yield PipelineData(None, str(NotImplementedError()), None)
