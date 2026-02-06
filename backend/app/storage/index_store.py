# 文件路径: storage/index_store.py

from dataclasses import dataclass, field
from typing import Dict, List, Tuple
import pickle
import os


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

    # =========================
    # 保存
    # =========================
    def save_to_disk(self, filepath: str):
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
            return pickle.load(f)
