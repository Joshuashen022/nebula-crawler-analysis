import json
from pathlib import Path
from typing import Any, Dict


def properties_not_empty(props: Any) -> bool:
    """
    判断 Properties 是否“非空”：
    - 不是 None
    - 不是空字典 {}
    - 不是空列表 []
    """
    if props is None:
        return False
    if isinstance(props, (dict, list)) and len(props) == 0:
        return False
    return True


def filter_visits_with_properties(
    input_path: Path,
    output_path: Path,
) -> None:
    """
    从 ndjson 中筛选出 Properties 不为空的整行，并写入到新的 ndjson 文件。
    """
    with input_path.open("r", encoding="utf-8") as fin, output_path.open(
        "w", encoding="utf-8"
    ) as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            try:
                obj: Dict[str, Any] = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not properties_not_empty(obj.get("Properties")):
                continue
            fout.write(line + "\n")


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    input_file = root / "results" / "2026-03-12T10:37_visits.ndjson"
    output_file = root / "results" / "2026-03-12T10:37_visits_with_properties.ndjson"
    filter_visits_with_properties(input_file, output_file)


if __name__ == "__main__":
    main()

