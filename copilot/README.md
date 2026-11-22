# LogPilot Copilot

Adapted from Elastic's [chatbot-rag-app](https://github.com/elastic/elasticsearch-labs/tree/main/example-apps/chatbot-rag-app)
(Apache-2.0, see `LICENSE-elastic-example`; upstream commit in `.upstream-commit`).

Changes from upstream so far (M1):
- `api/chat.py`: ELSER retrieval replaced by BM25 search over `logs-*`; each source links to the log line in Kibana Discover.
- `api/elasticsearch_client.py`: allows an unauthenticated local cluster.
- `api/templates/rag_prompt.txt`: log-evidence-only instructions.
- LLM: Groq via the existing `openai` integration (`OPENAI_BASE_URL` = Groq's OpenAI-compatible endpoint), set in `docker-compose.yml`.

Run: `docker compose up -d copilot`, then open http://localhost:4000.
