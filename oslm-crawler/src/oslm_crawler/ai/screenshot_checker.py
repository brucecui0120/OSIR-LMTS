"""
Check screenshots of repository pages on HuggingFace or Modelscope to determine whether 
the extraction of information such as model or dataset downloads is correct.
"""
import os
import yaml
import base64
from pathlib import Path
from langchain.chat_models import init_chat_model
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel, Field
from typing import Union, Literal, Optional
from dataclasses import dataclass, field

SCRIPT_PATH = Path(__file__)
ROOT_PATH = SCRIPT_PATH.parents[5]
CONFIG_PATH = ROOT_PATH / 'config/env.yaml'
with CONFIG_PATH.open('r', encoding='utf-8') as f:
    config = yaml.safe_load(f)
    if "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = config["OPENAI"][0]["OPENAI_API_KEY"]
    if "OPENAI_API_BASE" not in os.environ:
        os.environ["OPENAI_API_BASE"] = config["OPENAI"][0]["OPENAI_API_BASE"]

class ImageInfoHF(BaseModel):
    # link: str = Field(description="The link of the model")
    downloads_last_month: int | None = Field(default=None, description="The number of downloads in the last month")
    error: str | None = Field(default=None, description="Error message if extraction failed")
    
class ImageInfoMS(BaseModel):
    # link: str = Field(description="The link of the model")
    downloads: int | None = Field(default=None, description="The number of downloads of the model.")
    error: str | None = Field(default=None, description="Error message if extraction failed")
    
class ImageInfo(BaseModel):
    output: Union[ImageInfoHF, ImageInfoMS] = Field(description="The image information from HuggingFace or ModelScope")
    
@dataclass
class CheckRequest:
    img: str = field(init=False, metadata={"description": "The base64 encoded image data."})
    img_path: str
    link: str
    source: Literal["HuggingFace", "ModelScope"]
    
    def __post_init__(self):
        with open(self.img_path, "rb") as img_file:
            try:
                self.img = base64.b64encode(img_file.read()).decode('utf-8')
            except Exception as e:
                raise ValueError(f"Failed to read or encode image file: {e}")
    
    def to_dict(self):
        return {
            "source": self.source,
            "img": self.img
        }
    
@dataclass
class CheckResponse:
    link: str
    source: Literal["HuggingFace", "ModelScope"]
    downloads_last_month: Optional[int] = None
    downloads: Optional[int] = None
    error: Optional[str] = None

llm = init_chat_model("gpt-5", model_provider="openai")
prompt_template = ChatPromptTemplate.from_messages([
    {'role': 'system', 'content': ("You are an expert in computer vision and can accurately extract information from images. "
        "You will be provided with a screenshot containing information about a machine learning "
        "model from either HuggingFace or ModelScope. Your task is to analyze the screenshot "
        "and extract the number of downloads in the last month for HuggingFace models or the "
        "total number of downloads for ModelScope models. If you cannot obtain valid information "
        "from the image (for example, if the screenshot is a blank webpage), then you should set "
        "`downloads` or `downloads_last_month` to None, and explain the reason in the `error` field. "
        "If you can obtain information related to downloads, then you should keep the `error` field "
        "as None.")},
    {'role': 'user', 'content': [
        {"type": "text", "text": ("Analyze the screenshot and extract the required information. "
                                  "Only extract the properties mentioned in the `ImageInfo` class. "
                                  "Notice that the following screenshot is from a {source} repository.")},
        {"type": "image", "source_type": "base64", "data": "{img}", "mime_type": "image/png"}
    ]}
])

checker = llm.with_structured_output(ImageInfo, include_raw=True)
chain = prompt_template | checker

def check_image_info(requests: list[CheckRequest]) -> list[CheckResponse]:
    requests_dicts = [req.to_dict() for req in requests]
    responses = chain.batch_as_completed(requests_dicts)
    results = []
    for idx, res in responses:
        link = requests[idx].link
        source = requests[idx].source
        if res['parsing_error'] is not None:
            result = CheckResponse(link, source, None, None, error=res['parsing_error'])
        elif source == "HuggingFace":
            result = CheckResponse(link, source, res['parsed'].output.downloads_last_month, None, res['parsed'].output.error)
        elif source == "ModelScope":
            result = CheckResponse(link, source, None, res['parsed'].output.downloads, res['parsed'].output.error)
        else:
            result = CheckResponse(link, source, None, None, error="Unknown source")
        results.append(result)

    return results

if __name__ == "__main__":
    from pathlib import Path
    root_path = Path(__file__).parent.parent.parent

    requests = [
        CheckRequest(
            img_path=str(root_path / "test-hf.png"),
            link="https://huggingface.co/01-ai/Yi-1.5-9B-Chat",
            source="HuggingFace"
        ),
        CheckRequest(
            img_path=str(root_path / "test-ms.png"),
            link="https://modelscope.cn/models/BAAI/Automobile-llama3_1_8B_instruct",
            source="ModelScope"
        ),
        CheckRequest(
            img_path=str(root_path / "test-empty.png"),
            link="https://modelscope.cn/models/damo/cv_resnet50_image_classification_pytorch",
            source="ModelScope"
        )
    ]
    responses = check_image_info(requests)
    for res in responses:
        print(res)
    