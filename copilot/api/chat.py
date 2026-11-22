import json
import os

from elasticsearch_client import (
    elasticsearch_client,
    get_elasticsearch_chat_message_history,
)
from flask import current_app, render_template, stream_with_context
from functools import cache
from langchain_core.documents import Document
from llm_integrations import get_llm

# LogPilot M1: retrieve straight from the parsed logs-* indices with BM25.
# Week 7 replaces this with hybrid (BM25 + MiniLM kNN) search over rag-chunks.
INDEX = os.getenv("ES_INDEX", "logs-*")
INDEX_CHAT_HISTORY = os.getenv("ES_INDEX_CHAT_HISTORY", "chat-history")
KIBANA_URL = os.getenv("KIBANA_PUBLIC_URL", "http://localhost:5601")
TOP_K = int(os.getenv("RETRIEVAL_TOP_K", "20"))
SESSION_ID_TAG = "[SESSION_ID]"
SOURCE_TAG = "[SOURCE]"
DONE_TAG = "[DONE]"



def discover_url(doc_id):
    """Kibana Discover link that opens exactly this log line."""
    return (
        f"{KIBANA_URL}/app/discover#/?_g=(time:(from:now-7d,to:now))"
        f"&_a=(index:'logpilot-logs',query:(language:kuery,query:'_id:\"{doc_id}\"'))"
    )


def retrieve(question):
    hits = elasticsearch_client.search(
        index=INDEX,
        size=TOP_K,
        query={"multi_match": {"query": question, "fields": ["log_message^2", "message", "component", "service", "level"]}},
        sort=["_score", {"@timestamp": "desc"}],
        collapse={"field": "log_message.raw"},  # one hit per distinct line, not N replays of it
    )["hits"]["hits"]
    return [
        Document(
            page_content=h["_source"]["message"],
            metadata={
                "name": f"{h['_source'].get('service', '?')} {h['_source']['@timestamp']} [{h['_id'][:8]}]",
                "url": discover_url(h["_id"]),
                "category": h["_source"].get("service"),
                "updated_at": h["_source"]["@timestamp"],
            },
        )
        for h in hits
    ]


@cache
def get_lazy_llm():
    return get_llm()


@stream_with_context
def ask_question(question, session_id):
    llm = get_lazy_llm()

    yield f"data: {SESSION_ID_TAG} {session_id}\n\n"
    current_app.logger.debug("Chat session ID: %s", session_id)

    chat_history = get_elasticsearch_chat_message_history(
        INDEX_CHAT_HISTORY, session_id
    )

    if len(chat_history.messages) > 0:
        # create a condensed question
        condense_question_prompt = render_template(
            "condense_question_prompt.txt",
            question=question,
            chat_history=chat_history.messages,
        )
        condensed_question = llm.invoke(condense_question_prompt).content
    else:
        condensed_question = question

    current_app.logger.debug("Condensed question: %s", condensed_question)
    current_app.logger.debug("Question: %s", question)

    docs = retrieve(condensed_question)
    for doc in docs:
        doc_source = {**doc.metadata, "page_content": doc.page_content}
        current_app.logger.debug(
            "Retrieved document passage from: %s", doc.metadata["name"]
        )
        yield f"data: {SOURCE_TAG} {json.dumps(doc_source)}\n\n"

    qa_prompt = render_template(
        "rag_prompt.txt",
        question=question,
        docs=docs,
        chat_history=chat_history.messages,
    )

    answer = ""
    for chunk in llm.stream(qa_prompt):
        content = chunk.content.replace(
            "\n", " "
        )  # the stream can get messed up with newlines
        yield f"data: {content}\n\n"
        answer += chunk.content

    yield f"data: {DONE_TAG}\n\n"
    current_app.logger.debug("Answer: %s", answer)

    chat_history.add_user_message(question)
    chat_history.add_ai_message(answer)
