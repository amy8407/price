# ==============================================
# beef_all.py 수정 가이드 (3곳)
# ==============================================


# ── 변경 1: 파일 맨 위 import 영역에 추가 ──

import json                                          # 추가
from google.oauth2 import service_account            # 추가
from googleapiclient.discovery import build          # 추가
from googleapiclient.http import MediaFileUpload     # 추가


# ── 변경 2: main() 위에 업로드 함수 추가 ──

def upload_to_google_drive(file_path):
    """생성된 파일을 구글 드라이브에 업로드"""
    try:
        creds_json = os.environ.get('GDRIVE_CREDENTIALS')
        folder_id = os.environ.get('GDRIVE_FOLDER_ID')

        if not creds_json or not folder_id:
            print(f"[업로드 건너뜀] 환경변수 없음: {file_path}")
            return

        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)

        file_metadata = {
            'name': os.path.basename(file_path),
            'parents': [folder_id]
        }
        media = MediaFileUpload(file_path, resumable=True)

        result = service.files().create(
            body=file_metadata, media_body=media, fields='id'
        ).execute()
        print(f"업로드 완료: {os.path.basename(file_path)} (ID: {result.get('id')})")
    except Exception as e:
        print(f"업로드 실패: {file_path} - {e}")


# ── 변경 3: main() 함수 끝부분에 업로드 호출 추가 ──
# 기존:
#     print(f"\n=== 모든 작업 완료 ===")
#     print(f"가격 데이터: {price_filename}")
#     print(f"HTML: {html_file}")
#     print(f"Excel (비교): {excel_file}")
#     print(f"Excel (All Data): {all_data_file}")
#
# 변경 후:
#     print(f"\n=== 모든 작업 완료 ===")
#     # 구글 드라이브 업로드 (4개 파일)
#     for f in [price_filename, html_file, excel_file, all_data_file]:
#         if f:
#             upload_to_google_drive(f)
#
#     print(f"가격 데이터: {price_filename}")
#     print(f"HTML: {html_file}")
#     print(f"Excel (비교): {excel_file}")
#     print(f"Excel (All Data): {all_data_file}")


# ── 변경 4 (치명적): 파일 맨 아래 Windows 실행부 교체 ──
#
# [삭제] 이 블록 전체 삭제:
#   if __name__ == "__main__" and not os.environ.get('BEEF_ALL_RUNNING'):
#       os.environ['BEEF_ALL_RUNNING'] = '1'
#       os.system(f'cmd /k "chcp 65001 > nul && python "{__file__}""')
#       sys.exit()
#
# [교체] 아래 코드로 대체:

if __name__ == "__main__":
    # Windows 로컬 실행시 한글 깨짐 방지
    if os.name == 'nt' and not os.environ.get('BEEF_ALL_RUNNING'):
        os.environ['BEEF_ALL_RUNNING'] = '1'
        os.system(f'cmd /k "chcp 65001 > nul && python "{__file__}""')
        sys.exit()
    else:
        # GitHub Actions(Linux) 또는 이미 재실행된 Windows
        asyncio.run(main())
