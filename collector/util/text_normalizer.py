import unicodedata, re, hashlib
_WS = re.compile(r"\s+")
_ZERO_WIDTH = re.compile(r"[\u200B-\u200D\uFEFF]")  # ZWSP/ZWNJ/ZWJ/BOM
#
# def normalize_text(s: str) -> str:
#     if not s:
#         return ""
#     s = unicodedata.normalize("NFKC", s)
#     s = _ZERO_WIDTH.sub("", s)
#     s = s.replace("\u00A0", " ")
#     s = _WS.sub(" ", s)
#     return s.strip()
#
def normalize_text(s) -> str:
    if not s:
        return ""

    # 리스트나 dict 등 비문자열 → 문자열로 변환
    if not isinstance(s, str):
        if isinstance(s, list):
            s = " ".join(str(x) for x in s)
        else:
            s = str(s)

    # 유니코드 정규화 + 공백 처리
    s = unicodedata.normalize("NFKC", s)
    s = _ZERO_WIDTH.sub("", s)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = _WS.sub(" ", s).strip()
    return s
