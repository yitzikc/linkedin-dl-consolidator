# LinkedIn DL Consolidator

A simple Python package for consolidating LinkedIn data exports into a single JSON representation.

## Installation

You can install this package directly from GitHub:

```bash
pip install git+https://github.com/YOUR_USERNAME/linkedin-dl-consolidator.git
```

## Features

- Load LinkedIn `Connections.csv` and extract professional metadata (Company, Position).
- Merge messages with participant metadata.
- Convert message content from HTML to Markdown.
- Export as a structured JSON file for easy processing.

## Usage

### Command Line

Once installed, you can use the `linkedin-consolidate` command:

```bash
linkedin-consolidate --connections path/to/Connections.csv --messages path/to/messages.csv --output path/to/consolidated_data.json
```

### Python API

```python
from linkedin_dl_consolidator import run_consolidation

run_consolidation(
    connections_csv='path/to/Connections.csv',
    messages_csv='path/to/messages.csv',
    output_json='consolidated_data.json'
)
```

## Structure of JSON Export

The resulting JSON will have the following structure:

```json
[
    {
        "conversation_id": "...",
        "participants": [
            {
                "full_name": "...",
                "Company": "...",
                "Position": "..."
            }
        ],
        "messages": [
            {
                "sender": "...",
                "timestamp": "...",
                "content": "..."
            }
        ]
    }
]
```

## Requirements

- pandas
- markdownify
- beautifulsoup4
