# Open Source Large Model (OSLM) Influence Crawler & Analyzer

## 🌟 Overview

This project provides a comprehensive toolkit for crawling and analyzing data about open-source models and datasets from popular platforms like Hugging Face and ModelScope. It gathers key metrics such as download counts, likes, and other metadata to provide insights into the open-source AI landscape.

A key feature of this project is the use of AI to enrich the collected data. We generate valuable information, including the modality of models/datasets (e.g., text, image, audio) and the specific lifecycle stage of a dataset within the context of large model development (e.g., pre-training, fine-tuning).

By aggregating this data, our tools allow users to analyze and quantify the influence and contributions of different organizations, institutions, and companies in the open-source large model ecosystem.

## ✨ Features

- **Multi-Platform Crawling**: Scrapes essential data for models and datasets from Hugging Face, ModelScope, and other platforms.

- **AI-Powered Data Enrichment**: Automatically generates metadata like modality and dataset lifecycle stage.

- **Influence Analysis**: Provides tools to aggregate data for analyzing the impact of various contributors.

- **Data Visualization**: Includes a simple, interactive frontend built with Streamlit to explore the data.

- **Modern Tooling**: Built with `uv` for fast and reliable dependency management and environment setup.

## 🚀 Getting Started

### Prerequisites

`uv` is required to manage the project environment and dependencies.

### Installation

1. Clone the repository:

```
git clone [https://github.com/your-username/oslm-crawler.git](https://github.com/your-username/oslm-crawler.git)
cd oslm-crawler
```

2. Create a virtual environment and install dependencies using uv:

```
uv sync
```

## 💻 Usage

You can run commands using `uv run oslm-crawler ...` directly, or activate the virtual environment first (`source .venv/bin/activate`) and then use `oslm-crawler ...`.

1. Data Crawling & Post-processing

This command runs the complete data collection and processing pipeline.

```
uv run oslm-crawler crawl [pipeline] --config [CONFIG_PATH]
```

- [pipeline]: The specific pipeline to run.

- [CONFIG_PATH]: (Optional) Path to your custom configuration file. If not specified, the default configuration at `config/default_task.yaml` will be used.

2. Generate Leaderboard

This command generates a leaderboard based on predefined rules in the configuration file.

```
uv run oslm-crawler gen-rank --config [CONFIG_PATH]
```

- [CONFIG_PATH]: (Optional) Path to your configuration file. Defaults to config/default_task.yaml.

### Configuration

For detailed configuration options, please refer to the default config file: `config/default_task.yaml`. You can create your own config file and pass its path using the `--config` flag.

## 📊 Data Visualization Dashboard

This project includes a simple web interface built with Streamlit to visualize and interact with the collected data.

To start the dashboard, run the following command from the project's root directory:

```
streamlit run Home.py
```

Now you can open your browser and navigate to the local URL provided by Streamlit to view the dashboard.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue to discuss your ideas.
