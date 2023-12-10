from typing import Dict, Any


def format_dict_to_list(modified: Dict[str, Any]):
    # Format dict to be list of it's values
    modified_values = [modified_single for modified_single in modified.values()]
    return modified_values
