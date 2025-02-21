import hashlib
import json
import os
import re
from collections.abc import Sequence, Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import git


class Helper:

    @staticmethod
    def get_last_git_tag() -> str:
        repo = git.Repo(Helper.build_paths('.'), search_parent_directories=True)
        last_tag = repo.tags[0].name
        return last_tag

    @staticmethod
    def generate_dict_hash(data: dict, encondig: str = 'utf-8') -> str:
        stringified_data = Helper.recursively_stringify_objects(data)
        pairs_list = list(map(lambda key, value: f'{key}={value}',
                              stringified_data.keys(), stringified_data.values()))
        sorted_list = sorted(pairs_list)
        list_as_single_string = ''.join(sorted_list)
        encoded_string = list_as_single_string.encode(encondig)
        md5_hashlib = hashlib.md5()
        md5_hashlib.update(encoded_string)
        md5_generated_hash = md5_hashlib.hexdigest()
        return md5_generated_hash

    @staticmethod
    def ensure_folder(raw_path: str) -> Path:
        resolved_path = Helper.build_paths(raw_path)
        if not os.path.exists(resolved_path):
            os.makedirs(resolved_path)
        return resolved_path

    @staticmethod
    def build_paths(raw_strings: str | Sequence[str]) -> Path | list[Path]:
        def build_path(raw_string: str) -> Path:
            return Path(raw_string).resolve()

        if type(raw_strings) is str:
            return build_path(raw_strings)
        if isinstance(raw_strings, Sequence) and Helper.type_check_contents(raw_strings, str):
            return list(map(lambda raw_string: (build_path(raw_string)), raw_strings))

    @staticmethod
    def sanitize_name(raw_name: str) -> str:
        sanitized_name = re.sub(r'\W', '_', raw_name)
        return sanitized_name

    @staticmethod
    def type_check_contents(values: Sequence, expected_type: type) -> bool:
        if len(values) > 0 and all(map(lambda value: (isinstance(value, expected_type)), values)):
            return True
        else:
            return False

    @staticmethod
    def beautify_json(json_object: Any) -> str:
        stringified_objects = Helper.recursively_stringify_objects(json_object)
        return str(json.dumps(stringified_objects, indent=4))

    @staticmethod
    def recursively_stringify_objects(raw_container: Any) -> Any:
        def is_container(value):
            return isinstance(value, Sequence | dict) and type(value) is not str

        def evaluate(value):
            if is_container(value):
                return Helper.recursively_stringify_objects(value)
            elif isinstance(value, Path | datetime):
                return str(value)
            elif isinstance(value, Callable):
                return Helper.get_fully_qualified_name(value)
            else:
                return value

        if type(raw_container) is dict:
            return {key: (evaluate(value))
                    for key, value in zip(raw_container.keys(), raw_container.values())}
        elif is_container(raw_container):
            return list(map(lambda item: (evaluate(item)), raw_container))
        else:
            return evaluate(raw_container)

    @staticmethod
    def get_fully_qualified_name(obj: Callable) -> str:
        return f'{obj.__module__}.{obj.__qualname__}'
