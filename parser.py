from bs4 import BeautifulSoup


# Class/id markers that positively indicate a board area was rendered on the
# page (article top preview or a board page), used only to distinguish an
# observed empty board from an unknown/unavailable/restricted shape.
_BBS_AREA_MARKERS = (
    "st-bbs_reshead",
    "st-bbs_resbody",
    "st-bbsArea",
    "st-bbs_contents",
    "bbs_contents",
)
_BBS_AREA_ID_MARKERS = ("bbs", "bbs_contents")


def _coerce_res_no(raw) -> int | None:
    if raw is None:
        return None
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    if value < 0:
        return None
    return value


def _board_area_is_present(soup: BeautifulSoup) -> bool:
    """Return True only when a board area container is clearly rendered."""

    for element in soup.find_all(attrs={"class": True}):
        class_value = element.get("class") or []
        if isinstance(class_value, str):
            class_tokens = class_value.split()
        else:
            class_tokens = list(class_value)
        for token in class_tokens:
            for marker in _BBS_AREA_MARKERS:
                if token == marker or token.startswith(marker):
                    return True

    for element in soup.find_all(attrs={"id": True}):
        if element.get("id") in _BBS_AREA_ID_MARKERS:
            return True

    return False


def extract_observed_max_res_no(soup: BeautifulSoup) -> int | None:
    """Extract the observed maximum board response number from page HTML.

    Works on article-top preview soup or board/response-preview soup by
    reading every ``data-res_no`` attribute (the same signal ``parse_responses``
    relies on) and returning the maximum.

    Returns:
      - ``int`` (> 0) when one or more response numbers are visible.
      - ``0`` only when a board area is positively rendered but contains no
        response numbers (an observed empty board).
      - ``None`` for parse miss, unknown/ambiguous shape, or an
        unavailable/restricted board where no board area is detected.

    Never raises on unexpected HTML shapes.
    """

    if soup is None:
        return None

    try:
        res_numbers = [
            coerced
            for element in soup.find_all(attrs={"data-res_no": True})
            if (coerced := _coerce_res_no(element.get("data-res_no")))
            is not None
        ]
        if res_numbers:
            return max(res_numbers)

        if _board_area_is_present(soup):
            return 0
        return None
    except Exception:
        return None


def parse_responses(soup: BeautifulSoup) -> list:
    """
    掲示板HTMLからレス情報を抽出する。
    抽出対象:
      - レス番号
      - 投稿者名
      - 投稿日時
      - ID
      - 本文テキスト
      - 本文HTML（将来用保険）
    """

    responses = []
    res_heads = soup.find_all("dt", class_="st-bbs_reshead")

    for head in res_heads:

        res_no_raw = head.get("data-res_no")
        id_hash = head.get("data-id_hash")

        # 投稿者名
        name_tag = head.find("span", class_="st-bbs_name")
        poster_name = name_tag.get_text(strip=True) if name_tag else None

        # 投稿日時
        time_tag = head.find("span", class_="bbs_resInfo_resTime")
        posted_at = time_tag.get_text(strip=True) if time_tag else None

        body = head.find_next_sibling("dd", class_="st-bbs_resbody")

        content_text = ""
        content_html = ""

        if body:
            inner = body.find("div", class_="bbs_resbody_inner")

            if inner:

                # 本文以外の領域削除
                for cls in [
                    "st-bbs_contents-oekaki",
                    "st-bbs_contentsTitle",
                    "st-bbsArea_buttons",
                    "st-bbs_referLabel",   # ← 追加
                ]:
                    for tag in inner.find_all(class_=cls):
                        tag.decompose()

                # 画像や入力欄は本文扱いしない
                for tag in inner.find_all(["input", "img"]):
                    tag.decompose()

                # <br> を改行へ変換
                for br in inner.find_all("br"):
                    br.replace_with("\n")

                content_html = str(inner)

                # テキスト抽出（separator未使用）
                content_text = inner.get_text()
                content_text = content_text.strip()

                # 過剰改行圧縮
                while "\n\n\n" in content_text:
                    content_text = content_text.replace("\n\n\n", "\n\n")

        if not res_no_raw:
            continue

        responses.append({
            "res_no": int(res_no_raw),
            "id_hash": id_hash,
            "poster_name": poster_name,
            "posted_at": posted_at,
            "content": content_text,
            "content_html": content_html,
        })

    return responses
