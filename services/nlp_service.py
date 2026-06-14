from typing import List, Tuple, Dict
from pypinyin import pinyin, Style


def get_pinyin(text: List[str]) -> List[List[Dict[str, str]]]:
    total_res = []
    for page in text:
        zy = pinyin(page, style=Style.BOPOMOFO, errors='ignore')
        char_idx, zy_idx = 0, 0
        page_res = []
        while char_idx < len(page):
            char = page[char_idx]
            if 0x4E00 <= ord(char) <= 0x9FFF:
                ZY = zy[zy_idx][0]
                word_res = {"char": char, "zy": ZY}
                zy_idx += 1
            else:
                word_res = {"char": char, "zy": ""}
            char_idx += 1
            page_res.append(word_res)
        total_res.append(page_res)
    return total_res