from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional
import os
import pickle

from ..config import DATA_DIR
from ..schemas import Document


@dataclass
class DocumentStore:
    """A tiny persistent document store.

    Keys are external `doc_id` (string) to keep API consistent.
    """

    docs: Dict[str, Document] = field(default_factory=dict)
    _filepath: str = field(default_factory=lambda: str(DATA_DIR / "docs.pkl"))

    def __len__(self) -> int:
        return len(self.docs)

    def get(self, doc_id: str) -> Optional[Document]:
        return self.docs.get(doc_id)

    def all(self) -> List[Document]:
        # Keep deterministic order for reproducibility
        return list(self.docs.values())

    def add_documents(self, docs: Iterable[Document], persist: bool = False) -> int:
        """Add documents; de-duplicate by `doc_id`.

        Returns the number of newly ingested documents.
        """
        added = 0
        for d in docs:
            if d.doc_id in self.docs:
                continue
            self.docs[d.doc_id] = d
            added += 1

        if persist and added:
            self.save_to_disk(self._filepath)
        return added

    # -------------------------
    # persistence
    # -------------------------
    def save_to_disk(self, filepath: Optional[str] = None) -> None:
        fp = filepath or self._filepath
        os.makedirs(os.path.dirname(fp), exist_ok=True)
        with open(fp, "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def load_from_disk(cls, filepath: str) -> "DocumentStore":
        if not os.path.exists(filepath):
            return cls()
        with open(filepath, "rb") as f:
            obj = pickle.load(f)
        # Backward/forward compatibility: accept either a DocumentStore instance
        # or a raw dict (older code).
        if isinstance(obj, cls):
            return obj
        store = cls()
        if isinstance(obj, dict):
            store.docs = obj
        return store

    def load_if_exists(self) -> None:
        if not os.path.exists(self._filepath):
            return
        loaded = self.load_from_disk(self._filepath)
        self.docs = loaded.docs
