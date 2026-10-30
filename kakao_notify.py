"""
카카오톡 '나에게 보내기' 알림
- GitHub Actions 마지막 단계에서 실행됩니다.
- 돼지/한우 HTML 결과 2개를 구글 문서로 변환해 드라이브에 올리고, 그 링크를 카톡으로 보냅니다.
- 카카오 refresh 토큰(2달짜리)이 갱신되면 구글 드라이브 폴더의 작은 파일에 저장해서
  다음 실행 때 자동으로 이어 씁니다. (그래서 2달마다 다시 받을 필요가 없어요)
"""
import glob
import io
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
TOKEN_FILE_NAME = "kakao_refresh_token_DO_NOT_DELETE.txt"

REST_KEY = os.environ.get("KAKAO_REST_KEY", "").strip()
CLIENT_SECRET = os.environ.get("KAKAO_CLIENT_SECRET", "").strip()
ENV_REFRESH = os.environ.get("KAKAO_REFRESH_TOKEN", "").strip()
FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID", "").strip()


# ------------------------------------------------------------
# 카카오 API
# ------------------------------------------------------------
def post_form(url, data, headers=None):
    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded;charset=utf-8")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode("utf-8"))
        except Exception:
            return {"error": str(e)}
    except Exception as e:
        return {"error": str(e)}


def refresh_access_token(refresh_token):
    data = {
        "grant_type": "refresh_token",
        "client_id": REST_KEY,
        "refresh_token": refresh_token,
    }
    if CLIENT_SECRET:
        data["client_secret"] = CLIENT_SECRET
    res = post_form("https://kauth.kakao.com/oauth/token", data)
    if "access_token" in res:
        return res
    print(f"[카톡] 토큰 갱신 실패: {res.get('error')} {res.get('error_description', '')}")
    return None


# ------------------------------------------------------------
# 구글 드라이브 (refresh 토큰 보관 + 업로드 확인)
# ------------------------------------------------------------
def drive_service():
    token_json = os.environ.get("GDRIVE_TOKEN")
    if not token_json:
        return None
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        creds = Credentials.from_authorized_user_info(json.loads(token_json))
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        print(f"[카톡] 드라이브 연결 실패 (무시하고 진행): {e}")
        return None


def drive_find(svc, name):
    q = f"name = '{name}' and trashed = false"
    if FOLDER_ID:
        q += f" and '{FOLDER_ID}' in parents"
    res = svc.files().list(q=q, fields="files(id)", pageSize=1).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None


def drive_read_token(svc):
    file_id = drive_find(svc, TOKEN_FILE_NAME)
    if not file_id:
        return None, None
    data = svc.files().get_media(fileId=file_id).execute()
    return file_id, data.decode("utf-8").strip()


def drive_save_token(svc, file_id, token):
    from googleapiclient.http import MediaIoBaseUpload

    media = MediaIoBaseUpload(io.BytesIO(token.encode("utf-8")), mimetype="text/plain")
    if file_id:
        svc.files().update(fileId=file_id, media_body=media).execute()
    else:
        meta = {"name": TOKEN_FILE_NAME}
        if FOLDER_ID:
            meta["parents"] = [FOLDER_ID]
        svc.files().create(body=meta, media_body=media, fields="id").execute()
    print("[카톡] refresh 토큰을 드라이브에 저장했습니다.")


def get_access_token(svc):
    file_id, stored = None, None
    if svc:
        try:
            file_id, stored = drive_read_token(svc)
        except Exception as e:
            print(f"[카톡] 드라이브 토큰 읽기 실패 (무시): {e}")

    candidates = [t for t in [stored, ENV_REFRESH] if t]
    candidates = list(dict.fromkeys(candidates))  # 중복 제거
    for rt in candidates:
        res = refresh_access_token(rt)
        if not res:
            continue
        new_rt = res.get("refresh_token")
        if svc and (new_rt or rt != stored):
            try:
                drive_save_token(svc, file_id, new_rt or rt)
            except Exception as e:
                print(f"[카톡] 드라이브 토큰 저장 실패: {e}")
        return res["access_token"]
    return None


# ------------------------------------------------------------
# HTML 결과 2개 -> 구글 문서로 변환해서 링크 만들기
# (카톡은 파일 첨부가 안 되고, 드라이브의 .html 은 코드로 보여서 문서로 변환합니다)
# ------------------------------------------------------------
RESULTS = [
    ("돼지", "PORK_OUTCOME", "pork_margin_pivot_*.html"),
    ("한우", "BEEF_OUTCOME", "beef_margin_compare_*.html"),
]


def upload_html_as_doc(svc, html_path, title):
    from googleapiclient.http import MediaFileUpload

    meta = {"name": title, "mimeType": "application/vnd.google-apps.document"}
    if FOLDER_ID:
        meta["parents"] = [FOLDER_ID]
    media = MediaFileUpload(html_path, mimetype="text/html", resumable=False)
    f = svc.files().create(body=meta, media_body=media, fields="id,webViewLink").execute()
    return f.get("webViewLink") or f"https://docs.google.com/document/d/{f['id']}/edit"


def collect_results(svc, now):
    lines, buttons = [], []
    for label, outcome_env, pattern in RESULTS:
        outcome = os.environ.get(outcome_env, "")
        files = sorted(glob.glob(pattern))
        if not files:
            if outcome == "skipped":
                lines.append(f"{label}: 실행 안 됨")
            else:
                lines.append(f"{label}: 실패 (결과 없음)")
            continue
        link = None
        if svc:
            try:
                link = upload_html_as_doc(svc, files[-1], f"{label} 결과 {now:%Y-%m-%d}")
            except Exception as e:
                print(f"[카톡] {label} 문서 변환 실패: {e}")
        lines.append(f"{label}: 완료")
        if link:
            buttons.append({"title": f"{label} 결과 보기", "link": {"web_url": link, "mobile_web_url": link}})
    return lines, buttons


def send(access_token, text, buttons):
    fallback = (
        f"https://drive.google.com/drive/folders/{FOLDER_ID}" if FOLDER_ID else "https://drive.google.com"
    )
    if not buttons:
        run_url = os.environ.get("RUN_URL") or fallback
        buttons = [{"title": "실행 기록 보기", "link": {"web_url": run_url, "mobile_web_url": run_url}}]
    template = {
        "object_type": "text",
        "text": text[:200],
        "link": buttons[0]["link"],
        "buttons": buttons[:2],
    }
    res = post_form(
        "https://kapi.kakao.com/v2/api/talk/memo/default/send",
        {"template_object": json.dumps(template, ensure_ascii=False)},
        {"Authorization": f"Bearer {access_token}"},
    )
    if res.get("result_code") == 0:
        print("[카톡] 전송 완료")
    else:
        print(f"[카톡] 전송 실패: {res}")


def main():
    if not REST_KEY or not ENV_REFRESH:
        print("[카톡] KAKAO_REST_KEY / KAKAO_REFRESH_TOKEN 이 없어 건너뜁니다.")
        return
    svc = drive_service()
    token = get_access_token(svc)
    if not token:
        print("[카톡] 토큰을 받지 못했습니다. refresh 토큰을 새로 발급해 주세요.")
        return

    now = datetime.now(KST)
    day = "월화수목금토일"[now.weekday()]
    if os.environ.get("KAKAO_TEST"):
        text = f"[축산 가격 수집] 알림 테스트\n{now:%m/%d}({day}) {now:%H:%M}\n\n카톡 알림 연결이 정상이에요."
        buttons = []
    else:
        lines, buttons = collect_results(svc, now)
        text = "\n".join([f"[축산 가격 수집] {now:%m/%d}({day})", ""] + lines)
    print(text)
    send(token, text, buttons)


if __name__ == "__main__":
    main()
