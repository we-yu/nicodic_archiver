from bs4 import BeautifulSoup


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

