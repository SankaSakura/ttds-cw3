# 文件路径: storage/index_store.py

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
import pickle
import os

from ..config import DATA_DIR


@dataclass
class IndexStore:
    """
    索引数据结构
    """

    # 1. 倒排表
    # Term -> [(Internal_DocID, Frequency)]
    postings: Dict[str, List[Tuple[int, int]]] = field(default_factory=dict)

    # 2. 文档长度
    # Internal_DocID -> Length
    doc_len: Dict[int, int] = field(default_factory=dict)

    # 3. 位置索引
    # Term -> {Internal_DocID: [pos1, pos2, ...]}
    positions: Dict[str, Dict[int, List[int]]] = field(default_factory=dict)

    # 4. ID 映射
    doc_id_map: Dict[str, int] = field(default_factory=dict)
    reverse_doc_id_map: Dict[int, str] = field(default_factory=dict)

    # 5. 文档元数据
    doc_metadata: Dict[int, dict] = field(default_factory=dict)

    # 6. 版本号（用于health check / debugging）
    index_version: str = "0"

    _filepath: str = field(default_factory=lambda: str(DATA_DIR / "index.pkl"))

    def bump_version(self) -> str:
        try:
            v = int(self.index_version)
            v += 1
            self.index_version = str(v)
        except Exception:
            # fallback to a monotonic-ish string
            self.index_version = "1"
        return self.index_version

    # =========================
    # 保存
    # =========================
    def save_to_disk(self, filepath: Optional[str] = None):
        filepath = filepath or self._filepath
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "wb") as f:
            pickle.dump(self, f)

    # =========================
    # 加载
    # =========================
    @classmethod
    def load_from_disk(cls, filepath: str) -> "IndexStore":
        if not os.path.exists(filepath):
            return cls()

        with open(filepath, "rb") as f:
            obj = pickle.load(f)
        # Accept old pickles that may not contain new fields.
        if isinstance(obj, cls):
            if not getattr(obj, "index_version", None):
                obj.index_version = "0"
            if not getattr(obj, "_filepath", None):
                obj._filepath = filepath
            return obj
        return cls()

    def load_if_exists(self) -> None:
        if not os.path.exists(self._filepath):
            return
        loaded = self.load_from_disk(self._filepath)
        self.postings = loaded.postings
        self.doc_len = loaded.doc_len
        self.positions = loaded.positions
        self.doc_id_map = loaded.doc_id_map
        self.reverse_doc_id_map = loaded.reverse_doc_id_map
        self.doc_metadata = loaded.doc_metadata
        self.index_version = loaded.index_version

    def persist(self) -> None:
        self.save_to_disk(self._filepath)
