from pydantic import ValidationError


def parse_key(key: str) -> tuple[str, ...]:
    if not isinstance(key, str):
        raise ValidationError("The key should be a string.")
    if key.startswith("(") and key.endswith(")"):
        return tuple(col.strip() for col in key[1:-1].split(","))
    else:
        return (key,)


def convert_list_to_set(val: list[str]) -> set[str]:
    if not isinstance(val, list):
        raise ValidationError("Attribute should be a list.")
    return set(val)
