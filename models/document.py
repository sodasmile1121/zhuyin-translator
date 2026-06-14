from dataclasses import dataclass, field
from typing import List, Dict

@dataclass
class Document:
    text: List[str] = field(default_factory=list)
    char_zy: List[List[Dict[str, str]]] = field(default_factory=list)