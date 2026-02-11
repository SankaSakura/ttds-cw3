# 文件路径: indexing/tokenizer.py

import re
from typing import List

from nltk.stem import PorterStemmer

try:
    import nltk
    from nltk.corpus import stopwords
except Exception:  # pragma: no cover
    nltk = None
    stopwords = None


# =========================
# 停用词：优先使用 nltk（如果环境里已安装/已下载），否则使用最小内置表。
# 注意：不要在 import 阶段自动 nltk.download()（demo/离线环境会翻车）。
# =========================

_FALLBACK_STOP = {
    "the", "a", "an", "and", "or", "to", "of", "in", "on", "for", "with",
    "is", "are", "was", "were", "be", "as", "by", "at", "from", "that",
    "this", "it", "its", "into", "over", "under", "we", "you", "they",
}

# =========================
# 全局对象（提升性能）
# =========================
_stemmer = PorterStemmer()
if stopwords is not None and nltk is not None:
    try:
        _stop_words = set(stopwords.words("english"))
    except Exception:
        _stop_words = set(_FALLBACK_STOP)
else:
    _stop_words = set(_FALLBACK_STOP)


# =========================
# 英文分词函数
# =========================
def tokenize_en(text: str) -> List[str]:
    """
    标准分词流程：
    1. 小写化 + 正则分词
    2. 去停用词
    3. 词干提取 (stemming)
    """

    if not text:
        return []

    # 只保留字母和数字
    tokens = re.findall(r"[a-z0-9]+", text.lower())

    valid_tokens = []

    for t in tokens:
        if t not in _stop_words and len(t) > 1:
            stemmed = _stemmer.stem(t)
            valid_tokens.append(stemmed)

    return valid_tokens
