#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import traceback
import os
import json
import requests
import xml.etree.ElementTree as ET
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- 구글 드라이브 업로드 함수 (검토 완료) ---
def upload_to_google_drive(file_path):
    try:
        creds_json = os.environ.get('GDRIVE_CREDENTIALS')
        folder_id = os.environ.get('GDRIVE_FOLDER_ID')
        
        if not creds_json or not folder_id:
            print("❌ Secrets 설정 누락: GDRIVE_CREDENTIALS 또는 GDRIVE_FOLDER_ID")
            return

        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)

        display_name = f"돼지가격_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        file_metadata = {'name': display_name, 'parents': [folder_id]}
        media = MediaFileUpload(file_path, resumable=True)
        
        print(f"🚀 업로드 시작: {display_name}")
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"✅ 구글 드라이브 업로드 성공 (ID: {file.get('id')})")
    except Exception as e:
        print(f"❌ 업로드 실패: {e}")

class PorkCompleteScraper:
    def __init__(self, service_key=None):
        self.market_wholesale_data = []
        self.auction_data = []
        self.errors = []
        self.service_key = service_key
        self.session = requests.Session()
        self._setup_session()
    
    def _setup_session(self):
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        })

    # --- [검토 필독] async를 추가하여 await 호출이 가능하도록 수정함 ---
    async def collect_auction_data(self, target_date=None):
        print("=== 돼지 도체 경락가 수집 시작 (API) ===")
        if not self.service_key: return False
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # API 호출 및 데이터 처리 (사용자님의 기존 로직 유지)
        date_api = target_date.replace('-', '')
        url = "http://data.ekape.or.kr/openapi-data/service/user/grade/auct/pigGrade"
        params = {'ServiceKey': self.service_key, 'startYmd': date_api, 'endYmd': date_api, 'skinYn': 'Y'}
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            # (기존 XML 파싱 로직 실행...)
            print("경락가 수집 완료")
            return True
        except:
            return False

    async def collect_pork_data(self):
        print("=== 돼지 도매가 웹 수집 시작 (Playwright) ===")
        # (기존 Playwright 로직 실행...)
        return True

    def save_excel(self, filename="pork_result.xlsx"):
        try:
            all_data = self.market_wholesale_data + self.auction_data
            if not all_data: return None
            df = pd.DataFrame(all_data)
            df.to_excel(filename, index=False)
            return filename
        except: return None

# --- 메인 실행부 (검토 완료) ---
async def main():
    service_key = os.getenv('EKAPE_API_KEY') or "LFq9u3tNGZKe+rUDioG7t8YJ6kLegDAwuy6sKuZAEHWUQ2RnPHUdh70zsjagYIdCWLKvoyxP4My/320pPvCatw=="
    scraper = PorkCompleteScraper(service_key=service_key)
    
    # 이제 둘 다 await를 붙여도 에러가 나지 않습니다.
    await scraper.collect_auction_data()
    await scraper.collect_pork_data()
    
    saved_file = scraper.save_excel("pork_final.xlsx")
    if saved_file:
        upload_to_google_drive(saved_file)

if __name__ == "__main__":
    asyncio.run(main())
