from app.models.schemas import TestRagChunk


class EmbeddingVectorTransformer:
    """
    embedding_version에 따라 TestRagChunk에서 어떤 속성을 벡터화에 사용할지 결정
    - html 태그 제거
    - 속성 간 구분자 처리
    - meta 내부 속성 지원 등 확장 가능
    """

    @classmethod
    def transform(
            cls,
            chunk: TestRagChunk,
            embedding_version: int = 1,
            separator: str = " | "
    ) -> str:
        # version 별 사용 속성 정의
        if embedding_version == 1:
            target_props = ["name", "contents"]
        else:
            target_props = ["name", "contents"]

        texts = []
        for prop in target_props:
            value = getattr(chunk, prop, "") or ""

            # paragraph_contents가 html인 경우 태그 제거
            if prop == "contents" and chunk.contents == "html":
                value = cls.strip_html(value)

            texts.append(value.strip())

        return separator.join(filter(None, texts)).strip()

    @staticmethod
    def strip_html(html: str) -> str:
        from bs4 import BeautifulSoup
        """
        BeautifulSoup 기반 고도화된 HTML 태그 제거
        - <script>, <style> 제거
        - 특수 HTML 엔티티 처리
        - 줄바꿈 제거 및 공백 정리
        """
        soup = BeautifulSoup(html, "html.parser")

        # 제거할 태그 (script, style 등)
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        # 추가: img 태그 제거
        for img in soup.find_all("img"):
            img.decompose()

        # 텍스트 추출 + 공백 정리
        # noinspection PyArgumentList
        text = soup.get_text(separator=" ", strip=True)

        # 이중 공백 정리
        text = " ".join(text.split())

        return text
