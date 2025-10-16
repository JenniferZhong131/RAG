# RAG (SQL-first) — Minimal Prototype

This repo hosts a tiny, practical RAG playground for CS520.
It includes three scripts in `src/`:
- `src/ingest_custom.py`    — ingest/index docs (schema/FAQ/notes) into the retriever.
- `src/retriever_custom.py` — run retrieval/baseline experiments, write metrics.
- `src/test_sql.py`         — simple SQL tests / verification.

## Quickstart
1) Python 3.10+ recommended.
2) (Optional) virtualenv:
   python -m venv .venv && source .venv/bin/activate
3) Install deps:
   pip install -r requirements.txt
4) Index the corpus:
   python src/ingest_custom.py
5) Run retrieval (baseline / experiments):
   python src/retriever_custom.py
6) (Optional) SQL verification:
   python src/test_sql.py

Outputs will be under `results/` (ignored by git).
