"""
Use the web access tool that comes with the grok-3-all model to determine the model modality 
and whether it belongs to a large model through the repository link.
"""
import yaml
import os
import json
from openai import OpenAI
from pathlib import Path
from langchain.chat_models import init_chat_model
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from pydantic import BaseModel, Field
from typing import Literal, Optional

SCRIPT_PATH = Path(__file__)
ROOT_PATH = SCRIPT_PATH.parents[5]
CONFIG_PATH = ROOT_PATH / 'config/env.yaml'
with CONFIG_PATH.open('r', encoding='utf-8') as f:
    config = yaml.safe_load(f)
    if "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = config["OPENAI"][0]["OPENAI_API_KEY"]
    if "OPENAI_API_BASE" not in os.environ:
        os.environ["OPENAI_API_BASE"] = config["OPENAI"][0]["OPENAI_API_BASE"]

class ModelInfo(BaseModel):
    link: str = Field(description="The link of the model")
    modality: Optional[Literal["Language", "Speech", "Vision", "Multimodal", "Vector", "Protein", 
                               "3D", "Embodied"]] = Field(default=None, description="The modality of the model")
    is_large_model: Optional[bool] = Field(default=None, description="Whether the model is a large model.")
    
class ModelInfoList(BaseModel):
    infos: list[ModelInfo] = Field(description="The list of model information")


llm_web_search = init_chat_model("grok-3-all", model_provider="openai")
llm_json_parse = init_chat_model("gpt-5", model_provider="openai")

web_search_prompt = """\
You are an expert in modern machine learning and model classification. Your task is to search all the following model repository links (HuggingFace or Modelscope), and judge based on the webpage information:

Whether the model belongs to the "era of large models" — this means models based on Transformer or newer architectures, vector/embedding models, or other advanced architectures (not traditional ML or pre-Transformer models). T5, BERT and BERT-like architectures (e.g., RoBERTa, ALBERT, DistilBERT, etc.) do NOT count as large models, even though they are Transformer-based.

If it does belong, identify the modality of the model. Possible modalities are:

Language: Models for natural language processing (e.g., text generation, translation, understanding).
Speech: Models for speech recognition, synthesis, or audio understanding.
Vision: Models for image or video understanding, generation, or recognition.
Multimodal: Models combining two or more modalities (e.g., CLIP, text-to-image, text+speech).
Protein: Models for protein folding, biological sequences, or related scientific tasks.
Vector: Any embedding or reranker model, regardless of input type (text, image, etc.).
3D: Models that process or generate 3D data, point clouds, or spatial geometry.
Embodied: Models designed for robotics, or embodied AI. This includes large language models explicitly framed as robot brains or control systems.

Instructions:
If you do not have the ability to access the network, state that you cannot access the network.
If a web page is blank or contains no valid information, then it should be considered as not belonging to the large model.
If the model is not from the large-model era, clearly state so. If it is, output its modality from the above list.
Keep the correspondence between each link and the conclusions you give.

Model links:
{model_links}
"""

json_parse_prompt = """\
You are an expert skilled at extracting effective information. Your task is to extract information based on a summary text from web searches, following the format of the `ModelInfoList` class. This summary text describes information from a list of HuggingFace or Modelscope model repositories, including whether these models are large models or not, what the model's modality is, and the web link. 

Note that if the model is not a large model, you should set `modality=None`; If the text states that it cannot determine the modality of the model or whether it belongs to large models, then you should set the corresponding field to `None`.

The modality should be one of the following: Language, Speech, Vision, Multimodal, Vector, Protein, 3D, Embodied. 

Here is the summary text:
{web_search_result}
"""

web_search_prompt_template = ChatPromptTemplate.from_template(web_search_prompt)
json_parse_prompt_template = ChatPromptTemplate.from_template(json_parse_prompt)

json_parser = llm_json_parse.with_structured_output(ModelInfoList, include_raw=True)
chain = (
    web_search_prompt_template 
    | llm_web_search 
    | StrOutputParser()
    | {"web_search_result": lambda x: x}
    | json_parse_prompt_template 
    | json_parser
)


client = OpenAI(
    api_key=os.environ.get("MOONSHOT_API_KEY"),
    base_url="https://api.moonshot.cn/v1",
)


def gen_model_info_modelscope(urls: list[str]) -> list[ModelInfo]:
    system_prompt = """\
You are an expert in modern machine learning and model classification.

Your task:
- Use $web_search to look at each given ModelScope model link.
- For each model, judge whether it belongs to the "era of large models".
  * Large models = Transformer or newer architectures (LLMs, multimodal, vector/embedding, etc.)
  * Exclude: T5, BERT, RoBERTa, ALBERT, DistilBERT and other "pre-LLM" models.
- If the model is a large model, output its modality (choose from: Language, Speech, Vision, Multimodal, Vector, Protein, 3D, Embodied).
- If not large, set modality=null and is_large_model=false.
- If you cannot access or the webpage has no valid info, set both fields to null.

Return your results in the following JSON format:

{
  "infos": [
    {
      "link": "MODEL_LINK",
      "modality": "Language | Speech | Vision | Multimodal | Protein | Vector | 3D | Embodied | null",
      "is_large_model": true | false | null
    },
    ...
  ]
}
"""

    user_prompt = "Here are the model links:\n" + "\n".join(urls)

    completion = client.chat.completions.create(
        model="kimi-k2-0905-preview",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0,
        response_format={"type": "json_object"},
        tools=[
            {
                "type": "builtin_function",
                "function": {"name": "$web_search"},
            }
        ]
    )

    content = completion.choices[0].message.content
    data = json.loads(content)

    try:
        result = ModelInfoList(**data)
        return result.infos
    except Exception as e:
        print("Parsing error:", e)
        return [ModelInfo(link=url, modality=None, is_large_model=None) for url in urls]


def gen_model_info_huggingface(urls: list[str]) -> list[ModelInfo]:
    result = chain.invoke({"model_links": urls})
    if result['parsing_error'] is None:
        return result['parsed'].infos
    else:
        print("Parsing error:", result['parsing_error'])
        return [ModelInfo(link=url, modality=None, is_large_model=None) for url in urls]


if __name__ == "__main__":
    from pprint import pprint
    hf_urls = [
        "https://huggingface.co/nvidia/GR00T-N1-2B",
        "https://huggingface.co/microsoft/renderformer-v1.1-swin-large",
        "https://huggingface.co/TencentARC/InstantMesh",
        "https://huggingface.co/nvidia/Cosmos-Predict1-7B-WorldInterpolator",
        "https://huggingface.co/facebook/esm2_t33_650M_UR50D",
        "https://huggingface.co/AI4Chem/ChemLLM-7B-Chat",
        "https://huggingface.co/microsoft/Dayhoff-170m-UR50",
        "https://huggingface.co/openai/shap-e",
        "https://huggingface.co/BAAI/bge-m3",
        "https://huggingface.co/BAAI/RoboBrain2.0-3B",
        "https://huggingface.co/BAAI/bge-reranker-v2-m3",
        "https://huggingface.co/BAAI/bge-visualized", # error
        "https://huggingface.co/google-bert/bert-base-uncased",
        "https://huggingface.co/ByteDance-Seed/Seed-OSS-36B-Instruct",
        "https://huggingface.co/nvidia/NVIDIA-Nemotron-Nano-9B-v2",
        "https://huggingface.co/Qwen/Qwen-Image-Edit",
        "https://huggingface.co/google/gemma-3-270m", # error
        "https://huggingface.co/tencent/Hunyuan-GameCraft-1.0",
        "https://huggingface.co/facebook/opt-125m",
    ]
    ms_urls = [
        "https://modelscope.cn/models/BAAI/OpenSeek-Mid-v1",
        "https://modelscope.cn/models/Qwen/Qwen3-Next-80B-A3B-Instruct",
        "https://modelscope.cn/models/Tencent-Hunyuan/Hunyuan-MT-7B",
        "https://modelscope.cn/models/OpenBMB/MiniCPM-V-4_5",
        "https://modelscope.cn/models/meituan-longcat/LongCat-Flash-Chat",
        "https://modelscope.cn/models/ByteDance-Seed/Seed-OSS-36B-Instruct",
        "https://modelscope.cn/models/deepseek-ai/DeepSeek-V3.1",
        "https://modelscope.cn/models/Shanghai_AI_Laboratory/Intern-S1-mini"
    ]
    models = gen_model_info_modelscope(ms_urls)
    pprint(models)
