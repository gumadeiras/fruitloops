from __future__ import annotations


def sql_classify(expression: str) -> str:
    return f"""
    CASE
        WHEN upper({expression}) LIKE '%ORN%' THEN 'ORN'
        WHEN upper({expression}) LIKE '%PN%' OR upper({expression}) LIKE '%PROJECTION NEURON%' THEN 'PN'
        WHEN upper({expression}) LIKE '%LN%' OR upper({expression}) LIKE '%LOCAL%' THEN 'LN'
        WHEN upper({expression}) LIKE 'KC%' OR upper({expression}) LIKE '%KENYON%' THEN 'KC'
        WHEN upper({expression}) LIKE '%MBON%' THEN 'MBON'
        WHEN upper({expression}) LIKE '%APL%' THEN 'APL'
        WHEN upper({expression}) LIKE '%DAN%' OR upper({expression}) LIKE '%PAM%' OR upper({expression}) LIKE '%PPL%' THEN 'DAN'
        ELSE ''
    END
    """


def sql_glomerulus(expression: str) -> str:
    return f"""
    CASE
        WHEN regexp_extract({expression}, 'ORN_([^_;, ]+)', 1) != ''
        THEN regexp_extract({expression}, 'ORN_([^_;, ]+)', 1)
        WHEN regexp_extract({expression}, '(^|[ ;,])([^_;, ]+)_ORN', 2) != ''
        THEN regexp_extract({expression}, '(^|[ ;,])([^_;, ]+)_ORN', 2)
        WHEN regexp_extract({expression}, '(^|[ ;,])([^_;, ]+)_[lvm]?PN', 2) != ''
        THEN regexp_extract({expression}, '(^|[ ;,])([^_;, ]+)_[lvm]?PN', 2)
        WHEN regexp_extract({expression}, 'PN_([^_;, ]+)', 1) != ''
        THEN regexp_extract({expression}, 'PN_([^_;, ]+)', 1)
        ELSE ''
    END
    """


def sql_side(expression: str) -> str:
    return f"""
    CASE
        WHEN regexp_matches(upper({expression}), '(^|[ ;,])[^ ;,]*(_|-)R($|[ ;,])') THEN 'R'
        WHEN regexp_matches(upper({expression}), '(^|[ ;,])R($|[ ;,])') THEN 'R'
        WHEN upper({expression}) LIKE '% RIGHT%' THEN 'R'
        WHEN lower({expression}) = 'right' THEN 'R'
        WHEN regexp_matches(upper({expression}), '(^|[ ;,])[^ ;,]*(_|-)L($|[ ;,])') THEN 'L'
        WHEN regexp_matches(upper({expression}), '(^|[ ;,])L($|[ ;,])') THEN 'L'
        WHEN upper({expression}) LIKE '% LEFT%' THEN 'L'
        WHEN lower({expression}) = 'left' THEN 'L'
        ELSE ''
    END
    """


def classify_name(value: str | None) -> str:
    text = (value or "").upper()
    if "ORN" in text:
        return "ORN"
    if "PN" in text or "PROJECTION NEURON" in text:
        return "PN"
    if "LN" in text or "LOCAL" in text:
        return "LN"
    if "KENYON" in text or text.startswith("KC") or "_KC" in text:
        return "KC"
    if "MBON" in text:
        return "MBON"
    if "APL" in text:
        return "APL"
    if "DAN" in text or "PAM" in text or "PPL" in text:
        return "DAN"
    return ""


def infer_glomerulus(value: str | None) -> str:
    text = (value or "").replace(";", " ")
    for token in text.replace(",", " ").split():
        cleaned = token.strip("()[]{}")
        upper = cleaned.upper()
        if upper.startswith("ORN_"):
            return cleaned.split("_", 1)[1]
        if upper.endswith("_ORN"):
            return cleaned.rsplit("_", 1)[0]
        if "_LPN" in upper or "_VPN" in upper or "_MPN" in upper:
            return cleaned.split("_", 1)[0]
        if upper.startswith("PN_"):
            return cleaned.split("_", 1)[1]
    return ""


def infer_side(value: str | None) -> str:
    text = (value or "").upper().strip()
    for sep in (";", ",", " "):
        text = text.replace(sep, " ")
    tokens = [token.strip("()[]{}") for token in text.split()]
    for token in tokens:
        if token.endswith("_R") or token.endswith("-R"):
            return "R"
        if token.endswith("_L") or token.endswith("-L"):
            return "L"
        if token == "R":
            return "R"
        if token == "L":
            return "L"
    if " RIGHT" in f" {text}" or text == "RIGHT":
        return "R"
    if " LEFT" in f" {text}" or text == "LEFT":
        return "L"
    return ""
