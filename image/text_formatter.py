import re
import arabic_reshaper
from bidi.algorithm import get_display

_OPEN_QUOTES  = set('"\u201c\u00ab')
_CLOSE_QUOTES = set('"\u201d\u00bb')
_ALL_QUOTES   = _OPEN_QUOTES | _CLOSE_QUOTES


PROTECTED_NAMES: list = sorted([
    "محمد بن زايد آل نهيان",
    "خالد بن محمد بن زايد",
    "حمدان بن محمد بن زايد",
    "أحمد بن محمد بن راشد",
    "منصور بن محمد بن راشد",
    "ذياب بن محمد بن زايد",
    "محمد بن راشد",
    "منصور بن زايد",
    "حمدان بن محمد",
    "سيف بن زايد",
    "عبدالله بن زايد",
    "طحنون بن زايد",
    "هزاع بن زايد",
    "حمدان بن زايد",
    "سلطان القاسمي",
    "عمر بن زايد",
    "حميد بن راشد",
    "سعود بن صقر",
    "راشد بن سعود",
    "حمد الشرقي",
    "سلطان بن زايد",
    "نهيان بن زايد",
    "محمد بن زايد",
    "رئيس الدولة",
    "ولي عهد",
    "أبو الغيط",
    "أبوالغيط",
    "بن مكتوم",
    "بن راشد",
    "بن زايد",
    "بن محمد",
    "آل نهيان",
    "آل مكتوم",
], key=lambda s: -len(s))

_NBSP = "\u00a0"


def _protect_names(text: str) -> str:
    
    for name in PROTECTED_NAMES:
        protected = name.replace(" ", _NBSP)
        text = text.replace(name, protected)
    return text


def prepare_ar_text(text: str) -> str:

    text = _protect_names(text)
    reshaped = arabic_reshaper.reshape(text)
    result = str(get_display(reshaped))
    return result
    return result


def _tokenize(text: str) -> list:
    
    tokens: list[str] = []
    i = 0
    n = len(text)

    while i < n:
        ch = text[i]

        if ch in ' \t':
            i += 1
            continue

        if ch in _OPEN_QUOTES:

            j = i + 1
            while j < n and text[j] not in _CLOSE_QUOTES:
                j += 1
            if j < n:                           
                tokens.append(text[i: j + 1])
                i = j + 1
            else:                               
                k = i + 1
                while k < n and text[k] != ' ':
                    k += 1
                tokens.append(text[i:k])
                i = k
        else:
            
            j = i
            while j < n and text[j] not in ' \t' and text[j] not in _ALL_QUOTES:
                j += 1
            tokens.append(text[i:j])
            i = j

    return [t for t in tokens if t]


def _fix_orphans(lines: list) -> list:
    
    changed = True
    while changed and len(lines) > 1:
        changed = False

        if len(lines) > 1:
            first_words = [w for w in lines[0].replace(_NBSP, " ").split() if w]
            if len(first_words) == 1:
                lines = [lines[0] + " " + lines[1]] + lines[2:]
                changed = True
                continue

        if len(lines) > 1:
            last_words = [w for w in lines[-1].replace(_NBSP, " ").split() if w]
            if len(last_words) == 1:
                lines = lines[:-2] + [lines[-2] + " " + lines[-1]]
                changed = True
    return lines


def _line_width(draw, tokens: list, font) -> int:

    text = " ".join(tokens)
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]


def _greedy_wrap(draw, tokens: list, font, max_width: int) -> list:
    
    lines = []
    current = []
    for tok in tokens:
        candidate = current + [tok]
        if _line_width(draw, candidate, font) <= max_width:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = [tok]
    if current:
        lines.append(current)
    return lines


def _balanced_wrap(draw, tokens: list, font, max_width: int, n_lines: int) -> list:
    
    n = len(tokens)


    width = {}
    for i in range(n):
        w = 0
        for j in range(i, n):
            tok_w = draw.textbbox((0, 0), tokens[j], font=font)[2] - \
                    draw.textbbox((0, 0), tokens[j], font=font)[0]
            space_w = draw.textbbox((0, 0), " ", font=font)[2] - \
                      draw.textbbox((0, 0), " ", font=font)[0] if j > i else 0
            w += tok_w + space_w
            width[(i, j)] = w

    INF = float("inf")

    dp   = [[INF] * (n_lines + 1) for _ in range(n + 1)]
    split = [[0]  * (n_lines + 1) for _ in range(n + 1)]
    dp[n][0] = 0

    for i in range(n - 1, -1, -1):
        for k in range(1, n_lines + 1):
            for j in range(i, n):
                w = width[(i, j)]
                if w > max_width:
                    break   
                cost = max(w, dp[j + 1][k - 1])
                if cost < dp[i][k]:
                    dp[i][k]   = cost
                    split[i][k] = j + 1


    lines = []
    i, k = 0, n_lines
    while k > 0 and i < n:
        j = split[i][k]
        lines.append(tokens[i:j])
        i, k = j, k - 1


    if not lines:
        lines = _greedy_wrap(draw, tokens, font, max_width)

    return lines


def wrap_text(draw, text: str, font, max_width: int) -> list:
    
    tokens = _tokenize(text)
    if not tokens:
        return []

    greedy_lines = _greedy_wrap(draw, tokens, font, max_width)
    n_lines = len(greedy_lines)

    if n_lines <= 1:
        return [" ".join(tokens)]

    balanced = _balanced_wrap(draw, tokens, font, max_width, n_lines)

    result = [" ".join(toks) for toks in balanced if toks]
    result = result if result else [" ".join(tokens)]

    return _fix_orphans(result)


def fit_text(
    draw,
    text: str,
    font_path: str,
    max_width: int,
    max_height: int,
    max_font_size: int = 60,
    min_font_size: int = 26,
) -> tuple:
    
    from PIL import ImageFont

    for size in range(max_font_size, min_font_size - 1, -2):
        font        = ImageFont.truetype(font_path, size)
        lines       = wrap_text(draw, text, font, max_width)
        line_height = size + 22         

        total_height  = len(lines) * line_height
        longest_line  = max(
            (draw.textbbox((0, 0), ln, font=font)[2] - draw.textbbox((0, 0), ln, font=font)[0])
            for ln in lines
        ) if lines else 0

        if total_height <= max_height and longest_line <= max_width:
            return font, lines, line_height

    return None, None, None
