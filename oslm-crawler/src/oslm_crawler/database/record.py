from dataclasses import dataclass, field
from typing import Optional, Literal


@dataclass
class ModelRecord:
    org: str
    repo: str
    model_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Protein', 'Vector', '3D', 'Embodied']
    downloads_last_month: int
    likes: int
    community: int
    descendants: int
    date_crawl: str
    date_last_crawl: Optional[str]
    date_enter_db: str
    

@dataclass
class DatasetRecord:
    org: str
    repo: str
    dataset_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Embodied']
    lifecycle: Literal['Pre-training', 'Fine-tuning', 'Preference', 'Evaluation']
    downloads_last_month: int
    likes: int
    community: int
    dataset_usage: int
    date_crawl: str
    date_last_crawl: Optional[str]
    date_enter_db: str


@dataclass
class HFModelRecord:
    org: str
    repo: str
    model_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Protein', 'Vector', '3D', 'Embodied']
    downloads_last_month: int
    likes: int
    community: int
    descendants: int
    date_crawl: str
    date_last_crawl: Optional[str]
    date_enter_db: str
    link: str
    img_path: Optional[str]


@dataclass
class HFDatasetRecord:
    org: str
    repo: str
    dataset_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Embodied']
    lifecycle: Literal['Pre-training', 'Fine-tuning', 'Preference', 'Evaluation']
    downloads_last_month: int
    likes: int
    community: int
    dataset_usage: int
    date_crawl: str
    date_last_crawl: Optional[str]
    date_enter_db: str
    link: str
    img_path: Optional[str]


@dataclass
class MSModelRecord:
    org: str
    repo: str
    model_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Protein', 'Vector', '3D', 'Embodied']
    downloads_last_month: int
    total_downloads: int
    likes: int
    community: int
    date_crawl: str
    date_last_crawl: Optional[str]
    date_enter_db: str
    link: str
    img_path: Optional[str]


@dataclass
class MSDatasetRecord:
    org: str
    repo: str
    dataset_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Embodied']
    lifecycle: Literal['Pre-training', 'Fine-tuning', 'Preference', 'Evaluation']
    downloads_last_month: int
    total_downloads: int
    likes: int
    community: int
    date_crawl: str
    date_last_crawl: Optional[str]
    date_enter_db: str
    link: str
    img_path: Optional[str]

    
@dataclass
class OpenDataLabRecord:
    org: str
    repo: str
    dataset_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Embodied']
    lifecycle: Literal['Pre-training', 'Fine-tuning', 'Preference', 'Evaluation']
    downloads_last_month: int
    total_downloads: int
    likes: int
    date_crawl: str
    date_last_crawl: Optional[str]
    date_enter_db: str
    link: str
    
    
@dataclass
class BAAIDataRecord:
    org: str
    repo: str
    dataset_name: str
    modality: Literal['Language', 'Speech', 'Vision', 'Multimodal', 'Embodied']
    lifecycle: Literal['Pre-training', 'Fine-tuning', 'Preference', 'Evaluation']
    downloads_last_month: int
    total_downloads: int
    likes: int
    date_crawl: str
    date_last_crawl: Optional[str]
    date_enter_db: str
    link: str    
