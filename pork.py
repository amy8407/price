# ==============================================
# 수정 방법: 원본 pork_scraper.py 에서 딱 3곳만 변경
# ==============================================


# ── 변경 1: 파일 맨 위 import 영역에 4줄 추가 ──

import json                                          # 추가
from google.oauth2 import service_account            # 추가
from googleapiclient.discovery import build          # 추가
from googleapiclient.http import MediaFileUpload     # 추가


# ── 변경 2: main() 함수 위에 이 함수 추가 ──

def upload_to_google_drive(file_path):
    """생성된 Excel을 구글 드라이브에 업로드"""
    try:
        creds_json = os.environ.get('GDRIVE_CREDENTIALS')
        folder_id = os.environ.get('GDRIVE_FOLDER_ID')

        if not creds_json or not folder_id:
            print("[업로드 건너뜀] GDRIVE_CREDENTIALS 또는 GDRIVE_FOLDER_ID 환경변수 없음")
            return

        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)

        display_name = f"돼지가격_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        file_metadata = {'name': display_name, 'parents': [folder_id]}
        media = MediaFileUpload(file_path, resumable=True)

        print(f"구글 드라이브 업로드 중: {display_name}")
        result = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"업로드 완료! (파일ID: {result.get('id')})")
    except Exception as e:
        print(f"업로드 실패: {e}")


# ── 변경 3: main() 함수 안에서 save_excel() 뒤에 1줄 추가 ──
# 기존:
#     excel_success = scraper.save_excel()
#     if excel_success:
#         print("\n모든 작업이 성공적으로 완료되었습니다!")
#
# 변경 후:
#     excel_filename = f"pork_wholesale_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
#     excel_success = scraper.save_excel(excel_filename)
#     if excel_success:
#         upload_to_google_drive(excel_filename)    ← 이 한 줄 추가
#         print("\n모든 작업이 성공적으로 완료되었습니다!")
