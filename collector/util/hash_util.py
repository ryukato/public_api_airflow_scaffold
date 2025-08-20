import hashlib

class HashUtil:
    # noinspection PyTypeChecker
    @staticmethod
    def create_hash(value: str) -> str:
        return hashlib.md5(value.encode("utf-8")).hexdigest()

    @staticmethod
    def generate_hashed_item_key(values, exclude: list[str]) -> str:
        hash_source = []
        for key, value in sorted(values.items()):
            if key not in exclude:
                hash_source.append(str(value) if value is not None else "")
        combined = "|".join(hash_source)
        return HashUtil.create_hash(combined)

