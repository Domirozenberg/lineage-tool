---
description: Set up development environment
allowed-tools: Bash
---

Set up the development environment:

```bash
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

If `requirements.txt` does not exist, create a minimal one for the project. On Windows use `venv\Scripts\activate`.
