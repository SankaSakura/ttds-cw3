from __future__ import annotations

from collections import defaultdict
from typing import Iterable, List

from ..schemas import Document
from ..storage.index_store import IndexStore
from .tokenizer import tokenize_en


def _index_one(doc: Document, internal_id: int, index: IndexStore) -> None:
    """Add a single document into IndexStore (postings + positions + lengths + metadata).

    NOTE: This function assumes doc_id_map/reverse_doc_id_map are already set.
    """
    full_text = f"{doc.title} {doc.body}"
    tokens = tokenize_en(full_text)

    index.doc_len[internal_id] = len(tokens)

    term_freqs = defaultdict(int)
    term_positions = defaultdict(list)

    for pos, term in enumerate(tokens):
        term_freqs[term] += 1
        term_positions[term].append(pos)

    for term, freq in term_freqs.items():
        if term not in index.postings:
            index.postings[term] = []
            index.positions[term] = {}
        index.postings[term].append((internal_id, freq))
        index.positions[term][internal_id] = term_positions[term]

    index.doc_metadata[internal_id] = {
        "title": doc.title,
        "url": doc.url,
        "timestamp": doc.timestamp,
        "lang": doc.lang,
    }


def build_index(docs: Iterable[Document], index: IndexStore) -> str:
    """Rebuild the whole index from scratch."""
    # Reset all structures
    index.postings.clear()
    index.doc_len.clear()
    index.positions.clear()
    index.doc_id_map.clear()
    index.reverse_doc_id_map.clear()
    index.doc_metadata.clear()

    next_internal = 0
    for doc in docs:
        if doc.doc_id in index.doc_id_map:
            continue
        internal_id = next_internal
        index.doc_id_map[doc.doc_id] = internal_id
        index.reverse_doc_id_map[internal_id] = doc.doc_id
        _index_one(doc, internal_id, index)
        next_internal += 1

    index.bump_version()
    index.persist()
    return index.index_version


def update_index(new_docs: List[Document], index: IndexStore) -> str:
    """Incrementally add documents that are not already indexed."""
    next_internal = len(index.doc_id_map)

    added_any = False
    for doc in new_docs:
        if doc.doc_id in index.doc_id_map:
            continue
        internal_id = next_internal
        index.doc_id_map[doc.doc_id] = internal_id
        index.reverse_doc_id_map[internal_id] = doc.doc_id
        _index_one(doc, internal_id, index)
        next_internal += 1
        added_any = True

    if added_any:
        index.bump_version()
        index.persist()
    return index.index_version
