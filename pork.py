#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
돼지고기 완전판 스크래핑 프로그램 (표시자 방식 + 재시도)
- 금천미트 부분육 시장가격 (16개 부위)
- 축산물품질평가원 도체 경락가격 (육질/육량등급별)
- 적수원가/마진 계산 비교 (PorkMarginCalculatorCompare)
- Excel 파일로 통합 저장 + 구글 드라이브 업로드
"""

import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import traceback
import os
import requests
import xml.etree.ElementTree as ET
import xlsxwriter

# ★ 구글 드라이브 업로드용 (OAuth 방식)
import json
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


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
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
    
    def _get_element_text(self, element, tag, default=''):
        try:
            found = element.find(tag)
            return found.text.strip() if found is not None and found.text else default
        except:
            return default
        
    def log_error(self, section, error_msg):
        error_entry = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'section': section,
            'error': str(error_msg)
        }
        self.errors.append(error_entry)
        print(f"[오류] {section}: {error_msg}")

    def collect_auction_data(self, target_date=None):
        print("=== 돼지 도체 경락가 수집 시작 ===")
        if not self.service_key:
            self.log_error("경락가", "API 인증키가 필요합니다")
            return False
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        try:
            pork_success = self._collect_pork_auction_data_api(target_date)
            if pork_success:
                print(f"돼지 도체 경락가 수집 완료: {len(self.auction_data)}건")
                return True
            else:
                return False
        except Exception as e:
            self.log_error("경락가", f"전체 수집 실패: {e}")
            return False

    def _collect_pork_auction_data_api(self, date_str):
        base_date = datetime.strptime(date_str, '%Y-%m-%d')
        api_endpoints = [
            {
                'url': "http://data.ekape.or.kr/openapi-data/service/user/grade/auct/pigGrade",
                'params_func': lambda date_api: {
                    'ServiceKey': self.service_key,
                    'startYmd': date_api,
                    'endYmd': date_api,
                    'skinYn': 'Y',
                    'sexCd': '025003',
                    'egradeExceptYn': 'N'
                },
                'name': 'pigGrade_제주제외전국탕박'
            }
        ]
        
        for days_back in range(30):
            try_date = base_date - timedelta(days=days_back)
            try_date_str = try_date.strftime('%Y-%m-%d')
            date_api = try_date_str.replace('-', '')
            
            for api in api_endpoints:
                try:
                    print(f"    돼지 경락가 API 시도: {api['name']} ({try_date_str})")
                    params = api['params_func'](date_api)
                    response = self.session.get(api['url'], params=params, timeout=30)
                    response.raise_for_status()
                    root = ET.fromstring(response.text)
                    result_code = root.find('.//resultCode')
                    if result_code is not None and result_code.text in ['0000', '00']:
                        items = root.findall('.//item')
                        if items:
                            print(f"    {api['name']} API에서 {len(items)}개 항목 발견")
                            collected = False
                            for item in items:
                                grade_fields = ['gradeNm', 'gradeName', 'grade']
                                price_fields = ['c_1101eTotAmt', 'CTotAmt', 'auctAmt', 'price', 'avgPrice']
                                grade_nm = None
                                for field in grade_fields:
                                    grade_nm = self._get_element_text(item, field)
                                    if grade_nm: break
                                if not grade_nm: grade_nm = '전체'
                                
                                price_value = None
                                price_str = self._get_element_text(item, 'c_1101eTotAmt')
                                if price_str and price_str != '0':
                                    try: price_value = int(price_str.replace(',', ''))
                                    except ValueError: pass
                                if not price_value:
                                    for field in price_fields:
                                        price_str = self._get_element_text(item, field)
                                        if price_str and price_str != '0':
                                            try:
                                                price_value = int(price_str.replace(',', ''))
                                                if price_value > 0: break
                                            except ValueError: continue
                                
                                if grade_nm and price_value:
                                    quantity_fields = ['c_1101eTotCnt', 'CTotCnt', 'auctQty', 'qty', 'count', 'headCount', 'totalQty']
                                    quantity_value = None
                                    used_field = None
                                    for qty_field in quantity_fields:
                                        qty_str = self._get_element_text(item, qty_field)
                                        if qty_str and qty_str != '0':
                                            try:
                                                quantity_value = int(qty_str.replace(',', ''))
                                                if quantity_value > 0:
                                                    used_field = qty_field
                                                    break
                                            except ValueError: continue
                                    jeju_excluded = "제주제외" if used_field == 'c_1101eTotCnt' else "제주포함"
                                    if quantity_value:
                                        print(f"      원본 데이터: 등급='{grade_nm}', 가격={price_value:,}원, 두수={quantity_value:,}두({jeju_excluded})")
                                    else:
                                        print(f"      원본 데이터: 등급='{grade_nm}', 가격={price_value:,}원, 두수=미확인")
                                
                                if price_value and price_value > 0:
                                    quantity_value = 0
                                    quantity_source = "미확인"
                                    qty_str = self._get_element_text(item, 'c_1101eTotCnt')
                                    if qty_str and qty_str != '0':
                                        try:
                                            quantity_value = int(qty_str.replace(',', ''))
                                            quantity_source = "제주제외"
                                        except ValueError: pass
                                    if quantity_value == 0:
                                        qty_str = self._get_element_text(item, 'CTotCnt')
                                        if qty_str and qty_str != '0':
                                            try:
                                                quantity_value = int(qty_str.replace(',', ''))
                                                quantity_source = "제주포함"
                                            except ValueError: pass
                                    
                                    grade_simplified = grade_nm
                                    if '등외제외' in grade_nm: grade_simplified = '등외제외'
                                    elif '1+' in grade_nm and '1++' not in grade_nm: grade_simplified = '1+'
                                    elif grade_nm.startswith('1') and '+' not in grade_nm: grade_simplified = '1'
                                    elif grade_nm.startswith('2'): grade_simplified = '2'
                                    elif '등외' in grade_nm or 'E' in grade_nm: grade_simplified = '등외'
                                    
                                    valid_grades = ['1+', '1', '2', '등외', '등외제외']
                                    if grade_simplified in valid_grades:
                                        self.auction_data.append({
                                            'date': try_date_str,
                                            'source': '축산물품질평가원(제주제외전국)',
                                            'type': '도체경락가',
                                            '축종': '돼지',
                                            '부위': '전체',
                                            '등급': grade_simplified,
                                            'grade_detail': grade_nm,
                                            '가격': price_value,
                                            'kg당가격': f"{price_value:,}원",
                                            '두수': quantity_value,
                                            '두수소스': quantity_source,
                                            '가격소스': '제주제외전국탕박',
                                            '도축방식': '탕박'
                                        })
                                        quantity_info = f", 두수={quantity_value:,}두({quantity_source})" if quantity_value > 0 else ""
                                        print(f"    돼지 제주제외 탕박 경락가: {grade_simplified}등급, {price_value:,}원{quantity_info}")
                                        collected = True
                            
                            if collected:
                                print(f"돼지 도체 경락가 수집 성공: {try_date_str} - {api['name']}")
                                return True
                    
                    print(f"    돼지 경락가 API {api['name']} {try_date_str}: 데이터 없음")
                except Exception as e:
                    print(f"    돼지 경락가 API {api['name']} {try_date_str} 오류: {str(e)}")
                    continue
            
            print(f"돼지 경락가 {try_date_str}: 모든 API 실패, 이전 날짜 시도...")
        
        self.log_error("돼지경락가API", "30일간 모든 API에서 경락가 데이터를 찾을 수 없음")
        return False

    async def collect_pork_data(self, timeout=300):
        print("=== 돼지 도매가 수집 시작 ===")
        print("1. API 도매가 수집 중...")
        api_success = self.collect_pork_wholesale_data_api()
        print("2. 웹스크래핑 시장가 수집 중...")
        web_success = False
        try:
            async with async_playwright() as p:
                browser = await p.firefox.launch(headless=True, args=['--no-sandbox', '--disable-dev-shm-usage'])
                context = await browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
                page = await context.new_page()
                page.set_default_timeout(60000)
                try:
                    web_success = await asyncio.wait_for(self._collect_pork_market_data(page), timeout=timeout)
                except asyncio.TimeoutError:
                    print("돼지 웹스크래핑 시간 초과")
                except Exception as e:
                    print(f"돼지 웹스크래핑 오류: {e}")
                await browser.close()
        except Exception as e:
            self.log_error("돼지수집", f"웹스크래핑 실패: {e}")
            traceback.print_exc()
        
        if api_success or web_success or len(self.market_wholesale_data) > 0:
            total_count = len(self.market_wholesale_data)
            print(f"돼지 데이터 수집 완료: 시장가 {total_count}건")
            return True
        else:
            print("API와 웹스크래핑 모두 실패")
            return self._generate_fallback_data()

    def collect_pork_wholesale_data_api(self, target_date=None):
        if not self.service_key:
            print("API 인증키가 없어 도매가 API 수집 건너뜀")
            return False
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        base_date = datetime.strptime(target_date, '%Y-%m-%d')
        
        for days_back in range(8):
            try_date = base_date - timedelta(days=days_back)
            try_date_str = try_date.strftime('%Y-%m-%d')
            date_api = try_date_str.replace('-', '')
            url = "http://data.ekape.or.kr/openapi-data/service/user/grade/auct/pigJejuGrade"
            params = {'ServiceKey': self.service_key, 'delngDe': date_api}
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                root = ET.fromstring(response.text)
                result_code = root.find('.//resultCode')
                if result_code is not None and result_code.text in ['0000', '00']:
                    items = root.findall('.//item')
                    if items:
                        for item in items:
                            grade_name = self._get_element_text(item, 'gradeName', '1등급')
                            price = self._get_element_text(item, 'price', '0')
                            if price and price != '0':
                                try:
                                    price_value = int(price.replace(',', '')) if isinstance(price, str) else int(price)
                                    self.market_wholesale_data.append({
                                        'date': try_date_str, 'source': '축산물품질평가원(API)',
                                        'type': '공식도매가', '축종': '돼지', '부위': '전체',
                                        '등급': grade_name, '가격': price_value, 'kg당가격': f"{price_value}원"
                                    })
                                    print(f"    돼지 공식도매가 API: {grade_name}, {price_value:,}원")
                                except ValueError: continue
                        print(f"돼지 공식도매가 API 수집 성공: {try_date_str} ({len(items)}건)")
                        return True
                print(f"돼지 공식도매가 API {try_date_str}: 데이터 없음, 이전 날짜 시도...")
            except Exception as e:
                print(f"돼지 공식도매가 API {try_date_str} 오류: {str(e)}")
                continue
        print("돼지 공식도매가 API: 8일간 데이터를 찾을 수 없음")
        return False

    async def _collect_pork_market_data(self, page):
        try:
            all_parts = [
                "미박삼겹", "등심", "목심", "안심",
                "미박앞다리", "미박뒷다리", "등갈비", "갈비",
                "등심덧살", "갈매기", "항정",
                "미박앞사태", "미박뒷사태",
                "냉동등뼈", "냉동지방A", "냉동잡육A",
                "냉동앞장족", "냉동뒷장족",
                "냉동덜미살", "냉동막창", "냉동돈두롤"
            ]
            failed_parts = all_parts.copy()

            for attempt in range(1, 4):
                if not failed_parts: break
                if attempt > 1:
                    print(f"\n=== {attempt}차 재시도: {len(failed_parts)}개 부위 ===")
                parts_to_try = failed_parts.copy()
                failed_parts = []

                pork_url = "https://www.ekcm.co.kr/dp/subMain?dispCtgNo=31&dispCtgNm=%EA%B5%AD%EB%82%B4%EC%82%B0+%EB%8F%88%EC%9C%A1&leafCtgNo&dispCtgNoList"
                await page.goto(pork_url, wait_until='domcontentloaded', timeout=30000)
                await page.wait_for_selector('li.ctg-item', timeout=10000)
                await page.wait_for_timeout(1000)

                for i, part in enumerate(parts_to_try, 1):
                    print(f"[돼지 {i}/{len(parts_to_try)}] {part} 부위 수집 중...")
                    success = False
                    try:
                        click_result = await page.evaluate("""
                            (partName) => {
                                const listItems = document.querySelectorAll('li.ctg-item');
                                for (let li of listItems) {
                                    const categoryP = li.querySelector('p.category');
                                    if (categoryP && categoryP.textContent) {
                                        const text = categoryP.textContent.trim().replace(/\\s*\\(\\d+\\)\\s*$/, '').trim();
                                        if (text === partName) {
                                            const link = li.querySelector('a.ctg-link');
                                            if (link) {
                                                link.scrollIntoView({ block: 'center' });
                                                link.click();
                                                return { success: true, matched: text, original: categoryP.textContent.trim() };
                                            }
                                        }
                                    }
                                }
                                return { success: false };
                            }
                        """, part)

                        clicked = click_result.get('success', False)
                        if clicked:
                            print(f"    ✓ 클릭: '{click_result.get('matched', '')}' (원본: {click_result.get('original', '')})")
                        if not clicked:
                            print(f"    ✗ 부위를 찾을 수 없음")
                            failed_parts.append(part)
                            continue

                        await page.wait_for_load_state('domcontentloaded', timeout=10000)
                        print(f"    페이지 렌더링 대기 중...")
                        await page.wait_for_timeout(3000)

                        print(f"    상품 확인 중...")
                        product_found = False
                        soldout_found = False
                        for wait_check in range(30):
                            await page.wait_for_timeout(500)
                            check_result = await page.evaluate("""
                                () => {
                                    const prices = document.querySelectorAll('.pd-price.xs.c-primary');
                                    const soldoutWrap = document.querySelector('.soldout-wrap');
                                    const soldoutMsg = document.body.innerText.includes('상품이 모두 판매되었습니다');
                                    return { priceCount: prices.length, hasContent: prices.length > 0, hasSoldout: soldoutWrap !== null || soldoutMsg };
                                }
                            """)
                            if check_result['hasContent']:
                                print(f"    상품 로드 완료 ({(wait_check+1)*0.5}초, {check_result['priceCount']}개)")
                                product_found = True
                                break
                            elif check_result['hasSoldout']:
                                print(f"    품절 확인 ({(wait_check+1)*0.5}초)")
                                soldout_found = True
                                break

                        if soldout_found or not product_found:
                            print(f"    {'품절' if soldout_found else '재고 없음'} - 최종 판매가 확인 중...")
                            last_price = await page.evaluate("""
                                () => {
                                    const soldoutWrap = document.querySelector('.soldout-wrap');
                                    if (soldoutWrap) {
                                        const priceRow = soldoutWrap.querySelector('dl.row.price');
                                        if (priceRow) {
                                            const priceEl = priceRow.querySelector('.pd-price.c-primary');
                                            if (priceEl) {
                                                const match = priceEl.textContent.match(/([0-9,]+)/);
                                                if (match) { const price = parseInt(match[1].replace(/,/g, '')); if (price >= 100 && price <= 100000) return price; }
                                            }
                                        }
                                    }
                                    const allText = document.body.innerText;
                                    const lastPriceMatch = allText.match(/최종\\s*판매가[^0-9]*([0-9,]+)/i);
                                    if (lastPriceMatch) { const price = parseInt(lastPriceMatch[1].replace(/,/g, '')); if (price >= 100 && price <= 100000) return price; }
                                    return null;
                                }
                            """)
                            if last_price:
                                print(f"    ✓ 마지막 판매가: {last_price:,}원/kg")
                                price = last_price
                            else:
                                print(f"    ✗ 마지막 판매가도 찾을 수 없음")
                                await page.screenshot(path=f"error_{part}.png")
                                failed_parts.append(part)
                                continue
                        else:
                            await page.wait_for_timeout(1000)
                            old_price = await page.evaluate("""() => { const fp = document.querySelector('.product-unit, .d-flex'); if (fp) { const pe = fp.querySelector('.pd-price.xs.c-primary'); return pe ? pe.textContent.trim() : null; } return null; }""")
                            sort_clicked = await page.evaluate("""() => { const buttons = document.querySelectorAll('button'); for (let btn of buttons) { if (btn.textContent && btn.textContent.includes('Kg당') && btn.textContent.includes('낮은')) { btn.click(); return true; } } return false; }""")
                            if sort_clicked:
                                print(f"    정렬 버튼 클릭 (정렬 전: {old_price})")
                                for wait_count in range(10):
                                    await page.wait_for_timeout(500)
                                    new_price = await page.evaluate("""() => { const fp = document.querySelector('.product-unit, .d-flex'); if (fp) { const pe = fp.querySelector('.pd-price.xs.c-primary'); return pe ? pe.textContent.trim() : null; } return null; }""")
                                    if new_price and new_price != old_price:
                                        print(f"    정렬 완료 ({(wait_count+1)*0.5}초, 정렬 후: {new_price})")
                                        break
                                await page.wait_for_timeout(1000)
                            else:
                                print(f"    경고: 정렬 버튼 없음")
                                await page.wait_for_timeout(1500)

                            price = await page.evaluate("""
                                () => {
                                    const fp = document.querySelector('.product-unit, .d-flex');
                                    if (fp) { const pe = fp.querySelector('.pd-price.xs.c-primary'); if (pe) { const match = pe.textContent.match(/([0-9,]+)/); if (match) { const price = parseInt(match[1].replace(/,/g, '')); if (price >= 1000 && price <= 100000) return price; } } }
                                    return null;
                                }
                            """)

                        if price:
                            self.market_wholesale_data.append({
                                'date': datetime.now().strftime('%Y-%m-%d'), 'source': '금천미트',
                                'type': '시장도매가', '축종': '돼지', '부위': part,
                                '등급': '1등급', '가격': price, 'kg당가격': f"{price:,}원"
                            })
                            print(f"    ✓ 가격: {price:,}원/kg")
                            success = True
                        else:
                            print(f"    ✗ 가격을 찾을 수 없음")

                        if i < len(parts_to_try):
                            await page.goto(pork_url, wait_until='domcontentloaded', timeout=30000)
                            await page.wait_for_selector('li.ctg-item', timeout=10000)
                    except Exception as e:
                        print(f"    ✗ 오류: {e}")

                    if not success:
                        failed_parts.append(part)

                print(f"\n{attempt}차 완료: {len(parts_to_try) - len(failed_parts)}/{len(parts_to_try)}개 성공")
                if failed_parts and attempt < 3:
                    print(f"실패: {', '.join(failed_parts)}")

            self._calculate_satae_average()
            self._calculate_jangjok_average()
            return len(self.market_wholesale_data) > 0
        except Exception as e:
            self.log_error("돼지수집", f"오류: {e}")
            return False

    def _calculate_satae_average(self):
        print("\n=== 사태 평균 계산 ===")
        front_satae = back_satae = None
        for data in self.market_wholesale_data:
            if data['부위'] == '미박앞사태': front_satae = data['가격']; print(f"미박앞사태 가격: {front_satae:,}원/kg")
            elif data['부위'] == '미박뒷사태': back_satae = data['가격']; print(f"미박뒷사태 가격: {back_satae:,}원/kg")
        if front_satae and back_satae:
            avg_price = int((front_satae + back_satae) / 2)
            self.market_wholesale_data.append({'date': datetime.now().strftime('%Y-%m-%d'), 'source': '금천미트', 'type': '시장도매가', '축종': '돼지', '부위': '사태', '등급': '1등급', '가격': avg_price, 'kg당가격': f"{avg_price:,}원"})
            print(f"사태 평균 가격: {avg_price:,}원/kg")
        else:
            missing = []
            if not front_satae: missing.append('미박앞사태')
            if not back_satae: missing.append('미박뒷사태')
            print(f"사태 평균 계산 실패: {', '.join(missing)} 데이터 없음")

    def _calculate_jangjok_average(self):
        print("\n=== 장족 평균 계산 ===")
        front_jangjok = back_jangjok = None
        for data in self.market_wholesale_data:
            if data['부위'] == '냉동앞장족': front_jangjok = data['가격']; print(f"냉동앞장족 가격: {front_jangjok:,}원/kg")
            elif data['부위'] == '냉동뒷장족': back_jangjok = data['가격']; print(f"냉동뒷장족 가격: {back_jangjok:,}원/kg")
        if front_jangjok and back_jangjok:
            avg_price = int((front_jangjok + back_jangjok) / 2)
            self.market_wholesale_data.append({'date': datetime.now().strftime('%Y-%m-%d'), 'source': '금천미트', 'type': '시장도매가', '축종': '돼지', '부위': '장족', '등급': '1등급', '가격': avg_price, 'kg당가격': f"{avg_price:,}원"})
            print(f"장족 평균 가격: {avg_price:,}원/kg")
        else:
            missing = []
            if not front_jangjok: missing.append('냉동앞장족')
            if not back_jangjok: missing.append('냉동뒷장족')
            print(f"장족 평균 계산 실패: {', '.join(missing)} 데이터 없음")

    def _generate_fallback_data(self):
        print("임시 가격 사용 금지 - 정확한 웹스크래핑 데이터만 수집됨")
        self.log_error("돼지_웹스크래핑", "웹스크래핑 데이터 없음")
        return False

    def _clean_data_for_excel(self, data_list):
        cleaned_data = []
        for item in data_list:
            cleaned_item = {}
            for key, value in item.items():
                if key == '축종': cleaned_item['Species'] = value
                elif key == '부위': cleaned_item['Part'] = value
                elif key == '등급': cleaned_item['Grade'] = value
                elif key == '가격': cleaned_item['Price'] = value
                elif key == 'kg당가격': cleaned_item['Price_Per_Kg'] = value
                else: cleaned_item[key] = value
            cleaned_data.append(cleaned_item)
        return cleaned_data

    def save_excel(self, filename=None):
        if filename is None:
            filename = f"pork_wholesale_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        try:
            if not self.market_wholesale_data and not self.auction_data:
                print("저장할 데이터가 없습니다.")
                return False
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                all_data = []
                if self.market_wholesale_data: all_data.extend(self.market_wholesale_data)
                if self.auction_data: all_data.extend(self.auction_data)
                if all_data:
                    df = pd.DataFrame(all_data)
                    type_order = ['도체경락가', '시장도매가', '공식도매가']
                    part_order = ['미박삼겹', '등심', '목심', '안심', '미박앞다리', '미박뒷다리', '등갈비', '갈비', '등심덧살', '갈매기', '항정', '미박앞사태', '미박뒷사태', '사태', '냉동등뼈', '냉동지방A', '냉동잡육A', '냉동돈피', '냉동앞장족', '냉동뒷장족', '장족', '냉동덜미살', '냉동막창', '냉동돈두롤', '전체']
                    df['type'] = pd.Categorical(df['type'], categories=type_order, ordered=True)
                    df['부위'] = pd.Categorical(df['부위'], categories=part_order, ordered=True)
                    df_sorted = df.sort_values(['type', '부위'])
                    df_sorted.to_excel(writer, sheet_name='돼지_통합데이터', index=False)
            market_count = len(self.market_wholesale_data) if self.market_wholesale_data else 0
            auction_count = len(self.auction_data) if self.auction_data else 0
            print(f"\n=== Excel 파일 저장 완료 ===")
            print(f"파일명: {filename}")
            print(f"데이터: 도매가 {market_count}건, 도체경락가 {auction_count}건")
            return True
        except Exception as e:
            self.log_error("Excel저장", f"저장 실패: {e}")
            return False

    def print_summary(self):
        if not self.market_wholesale_data and not self.auction_data:
            print("수집된 데이터가 없습니다.")
            return
        print(f"\n=== 돼지 데이터 수집 결과 요약 ===")
        if self.auction_data:
            print(f"도체 경락가: {len(self.auction_data)}건")
            for data in self.auction_data:
                print(f"  - {data['등급']}등급: {data['가격']:,}원/kg")
        if self.market_wholesale_data:
            df = pd.DataFrame(self.market_wholesale_data)
            print(f"도매가: {len(df)}건")
            print(f"수집 부위: {df['부위'].nunique()}개")
            for part in sorted(df['부위'].unique()):
                part_df = df[df['부위'] == part]
                avg_price = part_df['가격'].mean()
                print(f"  - {part}: {avg_price:,.0f}원/kg")


# ============================================================
# PorkMarginCalculatorCompare 클래스 (marginp_compare.py 원본)
# - 돼지는 소와 달리 등급 구분이 없어 단일(1등급) 그룹으로만 계산
# ============================================================

class PorkMarginCalculatorCompare:
    def __init__(self, price_file, weight_file=None):
        self.price_file = price_file
        self.weight_file = weight_file or "pig.xlsx"
        self.grades = ["1"]  # 돼지는 등급 구분 없이 단일 그룹만 사용
        self.margins = [0.10, 0.20, 0.30, 0.40]

        # 뼈류 고정가격 (원/kg) - pig.xlsx에서 확인된 값
        self.bone_prices = {
            "돈피": 2000,
            "꼬리": 3000,
            "사골": 500,
            "A지방": 2000
        }

        # 뼈류 중량 데이터 (비육중량.txt 파일 기준)
        self.bone_parts_weights = [
            ['돈피', 1.5],
            ['꼬리', 0.13],
            ['사골', 1.5],
            ['A지방', 1.6]
        ]

    def load_data(self):
        """데이터 로드"""
        print("데이터 로딩 중...")
        try:
            # pork.py에서 저장하는 시트명은 '돼지_통합데이터'
            self.df_price = pd.read_excel(self.price_file, sheet_name='돼지_통합데이터')
            print(f"가격 데이터 로드 완료: {len(self.df_price)}건")
        except Exception as e:
            try:
                xl = pd.ExcelFile(self.price_file)
                sheet_name = xl.sheet_names[0]
                self.df_price = pd.read_excel(self.price_file, sheet_name=sheet_name)
                print(f"가격 데이터 로드 완료 ({sheet_name}): {len(self.df_price)}건")
            except Exception as e2:
                print(f"가격 데이터 로드 실패: {e2}")
                return False
        return True

    def get_auction_price(self):
        """경락가 데이터에서 등외제외 가격 추출"""
        try:
            auction_rows = self.df_price[
                (self.df_price['type'] == '도체경락가') &
                (self.df_price['grade_detail'] == '등외제외')
            ]
            if not auction_rows.empty:
                price = auction_rows['가격'].iloc[0]
                print(f"경락가 데이터 사용: 등외제외 {price:,}원/kg")
                return price
            else:
                print("등외제외 경락가 데이터를 찾을 수 없어 기본값 사용: 7,494원/kg")
                return 7494
        except Exception as e:
            print(f"경락가 추출 오류: {e}, 기본값 사용: 7,494원/kg")
            return 7494

    def prepare_data(self):
        """데이터 전처리"""
        self.auction_price = self.get_auction_price()

        if not self.df_price.empty:
            market_data_filtered = self.df_price[self.df_price['type'] == '시장도매가'].copy()
            if not market_data_filtered.empty:
                self.market_data = pd.DataFrame({
                    '부위': market_data_filtered['부위'].values,
                    '가격': market_data_filtered['가격'].values,
                    '등급': '1'
                })
                print(f"도매가 데이터 로드: {len(self.market_data)}건")
            else:
                self.market_data = pd.DataFrame()
        else:
            self.market_data = pd.DataFrame()

        if not self.market_data.empty:
            self.market_pivot = self.market_data.pivot_table(index="부위", columns="등급", values="가격", aggfunc="last")
        else:
            self.market_pivot = pd.DataFrame()

        self.parse_pig_weights()

        # 부대비용 (pig.xlsx의 고정값)
        self.overhead_default = 19500.0

        print("데이터 전처리 완료")
        return True

    def parse_pig_weights(self):
        """돼지 부위별 중량 데이터 (비육중량.txt 기준, 등급 구분 없음)"""
        # 냉도체중
        self.carcass_weights = {"1": 85.0}

        fixed_parts_weights = [
            ['삼겹살', 11.08],
            ['등심', 5.9],
            ['목심', 4.33],
            ['안심', 1.13],
            ['앞다리살', 7.71],
            ['뒷다리살', 14.92],
            ['등갈비', 0.93],
            ['갈비', 2.84],
            ['가브리살', 0.42],
            ['갈매기살', 0.26],
            ['항정살', 0.41],
            ['사태', 3.14],
            ['등뼈', 3.3],
            ['잡육', 1.82],
            ['단족', 2.34]
        ]

        parts_data = [{'부위': p[0], '1': p[1]} for p in fixed_parts_weights]
        self.cut_weights = pd.DataFrame(parts_data)

    def get_market_price(self, part, grade="1"):
        """부위별 도매가격 조회"""
        if part in self.bone_prices:
            return float(self.bone_prices[part])

        # 부위명 매핑 (marginp.py 부위명 -> pork.py 수집 부위명)
        part_mapping = {
            '삼겹살': '미박삼겹',
            '등심': '등심',
            '목심': '목심',
            '안심': '안심',
            '앞다리살': '미박앞다리',
            '뒷다리살': '미박뒷다리',
            '등갈비': '등갈비',
            '갈비': '갈비',
            '가브리살': '등심덧살',
            '갈매기살': '갈매기',
            '항정살': '항정',
            '사태': '사태',
            '등뼈': '냉동등뼈',
            '잡육': '냉동잡육A',
            '단족': '장족'
        }

        default_prices = {
            '가브리살': 15000, '앞다리살': 12000, '미사태': 13000, '항정살': 20000,
            '뒷다리살': 11000, '삼겹살': 18000, '등갈비': 16000, '갈매기살': 25000,
            '갈비': 14000, '등뼈': 8000, '잡육': 10000
        }

        market_part = part_mapping.get(part, part)

        if not self.market_pivot.empty and market_part in self.market_pivot.index:
            if grade in self.market_pivot.columns:
                val = self.market_pivot.loc[market_part, grade]
                if pd.notna(val):
                    return float(val)
            for fallback_grade in ["1"]:
                if fallback_grade in self.market_pivot.columns:
                    val = self.market_pivot.loc[market_part, fallback_grade]
                    if pd.notna(val):
                        return float(val)

        if part in default_prices:
            return float(default_prices[part])

        if part == '사태':
            print(f"    사태 부위 가격 없음, 등심 가격으로 대체")
            return self.get_market_price('등심', grade)

        return np.nan

    def compute_compare_table(self, grade="1"):
        """적수원가/마진 비교 계산 (경락가 기반 + 금천10% 할증)"""
        auction_price = self.auction_price
        carcass_weight = self.carcass_weights[grade]
        total_cost = carcass_weight * auction_price + self.overhead_default

        parts_data = []

        if grade in self.cut_weights.columns:
            for _, row in self.cut_weights.iterrows():
                try:
                    weight = float(row[grade])
                    if weight > 0:
                        parts_data.append({"부위": row["부위"], "중량(kg)": weight})
                except:
                    continue

        for bone_data in self.bone_parts_weights:
            bone_name, bone_weight = bone_data[0], bone_data[1]
            bone_price = self.bone_prices[bone_name]
            bone_value = bone_weight * bone_price
            parts_data.append({
                "부위": bone_name, "중량(kg)": bone_weight,
                "시장가격(원/kg)": bone_price, "시장가치(원)": bone_value
            })

        if not parts_data:
            return pd.DataFrame()

        df = pd.DataFrame(parts_data)

        bone_part_names = [b[0] for b in self.bone_parts_weights]
        mask_no_price = ~df["부위"].isin(bone_part_names)
        if mask_no_price.any():
            df.loc[mask_no_price, "시장가격(원/kg)"] = df.loc[mask_no_price, "부위"].apply(lambda x: self.get_market_price(x, grade))

        df = df.dropna(subset=["시장가격(원/kg)"]).reset_index(drop=True)
        if df.empty:
            return pd.DataFrame()

        mask_no_value = ~df["부위"].isin(bone_part_names)
        if mask_no_value.any():
            df.loc[mask_no_value, "시장가치(원)"] = df.loc[mask_no_value, "중량(kg)"] * df.loc[mask_no_value, "시장가격(원/kg)"]

        virtual_total = df["시장가치(원)"].sum()

        # === 경락가 기반 방식 (원가 그대로) ===
        df["적수비"] = df["시장가치(원)"] / virtual_total if virtual_total > 0 else 0.0
        df["적수합계(원)"] = total_cost * df["적수비"]
        df["경락가_적수원가(원/kg)"] = df["적수합계(원)"] / df["중량(kg)"]
        df["경락가_현재마진율(%)"] = np.round(((df["시장가격(원/kg)"] - df["경락가_적수원가(원/kg)"]) / df["경락가_적수원가(원/kg)"]) * 100, 1)
        for margin in self.margins:
            df[f"경락가_{int(margin*100)}%마진"] = np.round(df["경락가_적수원가(원/kg)"] * (1 + margin), 0).astype(int)

        # === 금천미트 10% 할증 방식 (시장가격을 10% 마진으로 가정하고 역산) ===
        df["금천10%_시장가격(원/kg)"] = df["시장가격(원/kg)"]
        df["금천10%_적수원가(원/kg)"] = df["금천10%_시장가격(원/kg)"] / 1.10
        df["금천10%_현재마진율(%)"] = 10.0
        for margin in self.margins:
            margin_price = df["금천10%_적수원가(원/kg)"] * (1 + margin)
            df[f"금천10%_{int(margin*100)}%마진"] = np.round(margin_price, 0).astype(int)

        df["적수원가_차이(원/kg)"] = df["경락가_적수원가(원/kg)"] - df["금천10%_적수원가(원/kg)"]
        df["적수원가_차이율(%)"] = np.round(((df["경락가_적수원가(원/kg)"] - df["금천10%_적수원가(원/kg)"]) / df["금천10%_적수원가(원/kg)"]) * 100, 1)

        df["등급"] = grade
        df["경락가(원/kg)"] = auction_price
        df["냉도체중(kg)"] = carcass_weight
        df["부대비용(원)"] = self.overhead_default
        df["총원가(원)"] = int(round(total_cost))

        cols = (["등급", "부위", "중량(kg)", "시장가격(원/kg)",
                "경락가_적수원가(원/kg)", "경락가_현재마진율(%)", "경락가_10%마진", "경락가_20%마진", "경락가_30%마진", "경락가_40%마진",
                "금천10%_적수원가(원/kg)", "금천10%_현재마진율(%)", "금천10%_10%마진", "금천10%_20%마진", "금천10%_30%마진", "금천10%_40%마진",
                "적수원가_차이(원/kg)", "적수원가_차이율(%)",
                "시장가치(원)", "적수비", "적수합계(원)", "경락가(원/kg)", "냉도체중(kg)", "부대비용(원)", "총원가(원)"])

        return df[cols]

    def generate_results(self):
        """단일 그룹에 대해 계산 실행 (돼지는 등급 구분 없음)"""
        print("적수원가/마진 비교 계산 중...")
        self.results = {}
        for grade in self.grades:
            result = self.compute_compare_table(grade)
            if not result.empty:
                self.results[grade] = result
                print(f"{grade} 등급: {len(result)}개 부위 계산 완료")
            else:
                print(f"{grade} 등급: 계산 실패")
        return len(self.results) > 0

    def export_html(self, filename=None):
        """HTML 결과 생성"""
        if filename is None:
            filename = f"pork_margin_pivot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

        sections = []
        grade_titles = {"1": "돼지 (등급 구분 없음)"}

        for grade, df in self.results.items():
            if df.empty:
                continue

            virtual_total = df["시장가치(원)"].sum()
            total_cost = df["총원가(원)"].iloc[0]
            total_weight = df["중량(kg)"].sum()

            header = f"""
            <div class='card'>
                <div class='card-header' style='background:#2980B9;color:white;padding:14px 20px;'>
                    <span style='font-size:18px;font-weight:700'>{grade_titles[grade]} - 적수원가 계산 방식 비교</span>
                    <span style='margin-left:10px;background:rgba(255,255,255,0.2);padding:2px 8px;border-radius:12px;'>
                        경락가 {df['경락가(원/kg)'].iloc[0]:,}원/kg
                    </span>
                    <span style='margin-left:8px;background:rgba(255,255,255,0.2);padding:2px 8px;border-radius:12px;'>
                        냉도체중 {df['냉도체중(kg)'].iloc[0]:,.2f}kg
                    </span>
                    <br><div style='margin-top:8px'>
                    <span style='background:rgba(255,255,255,0.2);padding:2px 8px;border-radius:12px;margin-right:8px'>
                        시장가치총액 {virtual_total:,}원
                    </span>
                    <span style='background:rgba(255,255,255,0.2);padding:2px 8px;border-radius:12px;margin-right:8px'>
                        총원가 {total_cost:,}원
                    </span>
                    <span style='background:rgba(255,255,255,0.2);padding:2px 8px;border-radius:12px;'>
                        부위합계 {total_weight:.2f}kg
                    </span>
                    </div>
                </div>
            """

            table_html = "<div style='overflow:auto'><table style='width:100%;border-collapse:collapse;font-size:12px'>"
            table_html += """
            <thead>
                <tr style='background:#fbfcfe'>
                    <th rowspan="2" style='padding:8px;border:1px solid #eee;text-align:center'>부위</th>
                    <th rowspan="2" style='padding:8px;border:1px solid #eee;text-align:center'>중량(kg)</th>
                    <th rowspan="2" style='padding:8px;border:1px solid #eee;text-align:center'>시장가격<br/>(원/kg)</th>
                    <th colspan="6" style='padding:8px;border:1px solid #eee;text-align:center;background:#ffeaa7'>경락가 기반 (원가 그대로)</th>
                    <th colspan="5" style='padding:8px;border:1px solid #eee;text-align:center;background:#a8e6cf'>금천미트 10% 할증</th>
                    <th colspan="2" style='padding:8px;border:1px solid #eee;text-align:center;background:#ffb3ba'>차이 분석</th>
                </tr>
                <tr style='background:#fbfcfe'>
                    <th style='padding:6px;border:1px solid #eee;background:#ffeaa7'>적수원가</th>
                    <th style='padding:6px;border:1px solid #eee;background:#ffeaa7'>현재마진율</th>
                    <th style='padding:6px;border:1px solid #eee;background:#ffeaa7'>10%마진</th>
                    <th style='padding:6px;border:1px solid #eee;background:#ffeaa7'>20%마진</th>
                    <th style='padding:6px;border:1px solid #eee;background:#ffeaa7'>30%마진</th>
                    <th style='padding:6px;border:1px solid #eee;background:#ffeaa7'>40%마진</th>
                    <th style='padding:6px;border:1px solid #eee;background:#a8e6cf'>적수원가</th>
                    <th style='padding:6px;border:1px solid #eee;background:#a8e6cf'>10%마진</th>
                    <th style='padding:6px;border:1px solid #eee;background:#a8e6cf'>20%마진</th>
                    <th style='padding:6px;border:1px solid #eee;background:#a8e6cf'>30%마진</th>
                    <th style='padding:6px;border:1px solid #eee;background:#a8e6cf'>40%마진</th>
                    <th style='padding:6px;border:1px solid #eee;background:#ffb3ba'>원가차이</th>
                    <th style='padding:6px;border:1px solid #eee;background:#ffb3ba'>차이율(%)</th>
                </tr>
            </thead><tbody>
            """

            for _, row in df.iterrows():
                diff_color = '#ffe6e6' if row['적수원가_차이(원/kg)'] > 0 else '#e6ffe6'
                table_html += f"""
                <tr>
                    <td style='padding:6px;border:1px solid #eee'>{row['부위']}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right'>{row['중량(kg)']:,.2f}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;font-weight:bold'>{int(row['시장가격(원/kg)']):,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:#ffeaa7'>{int(row['경락가_적수원가(원/kg)']):,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:#ffeaa7'>{row['경락가_현재마진율(%)']:.1f}%</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:#ffeaa7'>{row['경락가_10%마진']:,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:#ffeaa7'>{row['경락가_20%마진']:,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:#ffeaa7'>{row['경락가_30%마진']:,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:#ffeaa7'>{row['경락가_40%마진']:,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:#a8e6cf'>{int(row['금천10%_적수원가(원/kg)']):,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:#a8e6cf'>{row['금천10%_10%마진']:,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:#a8e6cf'>{row['금천10%_20%마진']:,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:#a8e6cf'>{row['금천10%_30%마진']:,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:#a8e6cf'>{row['금천10%_40%마진']:,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:{diff_color};font-weight:bold'>{int(row['적수원가_차이(원/kg)']):,}</td>
                    <td style='padding:6px;border:1px solid #eee;text-align:right;background:{diff_color};font-weight:bold'>{row['적수원가_차이율(%)']:+.1f}%</td>
                </tr>
                """

            table_html += "</tbody></table></div></div>"
            sections.append(header + table_html)

        css = """
        body{font-family:'Malgun Gothic',system-ui,sans-serif;margin:20px;background:#f5f7fb}
        .card{background:white;margin:20px 0;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.1);overflow:hidden}
        """

        html_content = f"""
        <!DOCTYPE html>
        <html lang="ko">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>돼지 적수원가 계산 방식 비교</title>
            <style>{css}</style>
        </head>
        <body>
            <h1>돼지 적수원가 계산 방식 비교</h1>
            <div style='background:#e8f4f8;padding:15px;border-radius:8px;margin:20px 0'>
                <h3 style='margin-top:0;color:#2c3e50'>📊 계산 방식 설명</h3>
                <div style='display:flex;gap:20px'>
                    <div style='flex:1;background:#ffeaa7;padding:10px;border-radius:5px'>
                        <strong>🔸 경락가 기반 (원가 그대로)</strong><br/>
                        경매가 × 냉도체중 + 부대비용 = 총원가<br/>
                        적수비 적용 → 적수원가 계산
                    </div>
                    <div style='flex:1;background:#a8e6cf;padding:10px;border-radius:5px'>
                        <strong>🔸 금천미트 10% 할증</strong><br/>
                        시장가격을 10% 마진으로 가정<br/>
                        적수원가 = 시장가격 ÷ 1.10
                    </div>
                </div>
            </div>
            <p style='color:#666'>생성시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            {''.join(sections)}
        </body>
        </html>
        """

        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"HTML 결과 저장: {filename}")
        return filename

    def export_excel(self, filename=None):
        """Excel 결과 생성 (등급별 시트 + All_Data 시트, 돼지는 단일 그룹만 존재)"""
        if filename is None:
            filename = f"pork_margin_pivot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        wb = xlsxwriter.Workbook(filename)

        fmt_header = wb.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#fbfcfe'})
        fmt_text = wb.add_format({'border': 1})
        fmt_number = wb.add_format({'border': 1, 'num_format': '#,##0'})
        fmt_decimal = wb.add_format({'border': 1, 'num_format': '#,##0.00'})
        fmt_percent = wb.add_format({'border': 1, 'num_format': '0.0'})
        fmt_original = wb.add_format({'border': 1, 'num_format': '#,##0', 'bg_color': '#ffeaa7'})
        fmt_new = wb.add_format({'border': 1, 'num_format': '#,##0', 'bg_color': '#a8e6cf'})
        fmt_diff_pos = wb.add_format({'border': 1, 'num_format': '#,##0', 'bg_color': '#ffe6e6', 'bold': True})
        fmt_diff_neg = wb.add_format({'border': 1, 'num_format': '#,##0', 'bg_color': '#e6ffe6', 'bold': True})

        grade_titles = {"1": "돼지 (등급 구분 없음)"}
        all_data_rows = []

        for grade, df in self.results.items():
            if df.empty:
                continue

            ws = wb.add_worksheet(grade)

            header_format = wb.add_format({
                'bold': True, 'font_color': 'white', 'bg_color': '#2980B9',
                'align': 'left', 'font_size': 12, 'border': 1
            })
            ws.merge_range(0, 0, 0, 15, f"{grade_titles[grade]} - 적수원가 계산 방식 비교", header_format)

            ws.write(1, 0, "경락가(원/kg)", fmt_header)
            ws.write(1, 1, df['경락가(원/kg)'].iloc[0], fmt_number)
            ws.write(1, 2, "냉도체중(kg)", fmt_header)
            ws.write(1, 3, df['냉도체중(kg)'].iloc[0], fmt_decimal)
            ws.write(1, 4, "부대비용(원)", fmt_header)
            ws.write(1, 5, df['부대비용(원)'].iloc[0], fmt_number)

            virtual_total = df["시장가치(원)"].sum()
            total_weight = df["중량(kg)"].sum()
            ws.write(2, 0, "시장가치총액(원)", fmt_header)
            ws.write(2, 1, virtual_total, fmt_number)
            ws.write(2, 2, "총원가(원)", fmt_header)
            ws.write(2, 3, df['총원가(원)'].iloc[0], fmt_number)
            ws.write(2, 4, "부위합계(kg)", fmt_header)
            ws.write(2, 5, total_weight, fmt_decimal)

            ws.merge_range(4, 0, 5, 0, "부위", fmt_header)
            ws.merge_range(4, 1, 5, 1, "중량(kg)", fmt_header)
            ws.merge_range(4, 2, 5, 2, "시장가격(원/kg)", fmt_header)

            fmt_original_header = wb.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#ffeaa7'})
            ws.merge_range(4, 3, 4, 8, "경락가 기반 (원가 그대로)", fmt_original_header)

            fmt_new_header = wb.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#a8e6cf'})
            ws.merge_range(4, 9, 4, 13, "금천미트 10% 할증", fmt_new_header)

            fmt_diff_header = wb.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#ffb3ba'})
            ws.merge_range(4, 14, 4, 15, "차이 분석", fmt_diff_header)

            headers = ["적수원가", "현재마진율", "10%마진", "20%마진", "30%마진", "40%마진",
                      "적수원가", "현재마진율", "10%마진", "20%마진", "30%마진", "40%마진",
                      "원가차이", "차이율(%)"]
            for j, header in enumerate(headers, start=3):
                ws.write(5, j, header, fmt_header)

            for i, (_, row) in enumerate(df.iterrows(), start=6):
                ws.write(i, 0, row['부위'], fmt_text)
                ws.write(i, 1, float(row['중량(kg)']), fmt_decimal)
                ws.write(i, 2, float(row['시장가격(원/kg)']), fmt_number)

                ws.write(i, 3, float(row['경락가_적수원가(원/kg)']), fmt_original)
                ws.write(i, 4, float(row['경락가_현재마진율(%)']), fmt_percent)
                ws.write(i, 5, int(row['경락가_10%마진']), fmt_original)
                ws.write(i, 6, int(row['경락가_20%마진']), fmt_original)
                ws.write(i, 7, int(row['경락가_30%마진']), fmt_original)
                ws.write(i, 8, int(row['경락가_40%마진']), fmt_original)

                ws.write(i, 9, float(row['금천10%_적수원가(원/kg)']), fmt_new)
                ws.write(i, 10, float(row['금천10%_현재마진율(%)']), fmt_percent)
                ws.write(i, 11, int(row['금천10%_10%마진']), fmt_new)
                ws.write(i, 12, int(row['금천10%_20%마진']), fmt_new)
                ws.write(i, 13, int(row['금천10%_30%마진']), fmt_new)

                diff_fmt = fmt_diff_pos if row['적수원가_차이(원/kg)'] > 0 else fmt_diff_neg
                ws.write(i, 14, float(row['적수원가_차이(원/kg)']), diff_fmt)
                ws.write(i, 15, float(row['적수원가_차이율(%)']), diff_fmt)

                today = datetime.now().strftime('%Y-%m-%d')
                part = row['부위']

                all_data_rows.append({
                    'date': today, 'source': '경락가기준(적수비방식)', 'type': '적수원가',
                    'Species': '돼지', 'Part': part, 'Grade': grade,
                    'Price': int(row['경락가_적수원가(원/kg)']), 'Price_Per_Kg': f"{int(row['경락가_적수원가(원/kg)']):,}원"
                })
                for margin in [10, 20, 30, 40]:
                    all_data_rows.append({
                        'date': today, 'source': '경락가기준(적수비방식)', 'type': f'{margin}%마진',
                        'Species': '돼지', 'Part': part, 'Grade': grade,
                        'Price': int(row[f'경락가_{margin}%마진']), 'Price_Per_Kg': f"{int(row[f'경락가_{margin}%마진']):,}원"
                    })

                all_data_rows.append({
                    'date': today, 'source': '금천미트(10%마진가정)', 'type': '적수원가',
                    'Species': '돼지', 'Part': part, 'Grade': grade,
                    'Price': int(row['금천10%_적수원가(원/kg)']), 'Price_Per_Kg': f"{int(row['금천10%_적수원가(원/kg)']):,}원"
                })
                for margin in [10, 20, 30, 40]:
                    all_data_rows.append({
                        'date': today, 'source': '금천미트(10%마진가정)', 'type': f'{margin}%마진',
                        'Species': '돼지', 'Part': part, 'Grade': grade,
                        'Price': int(row[f'금천10%_{margin}%마진']), 'Price_Per_Kg': f"{int(row[f'금천10%_{margin}%마진']):,}원"
                    })

            ws.set_column(0, 0, 12)
            ws.set_column(1, 1, 10)
            ws.set_column(2, 15, 11)
            ws.freeze_panes(6, 1)

        if all_data_rows:
            ws_all = wb.add_worksheet('All_Data')
            headers_all = ['date', 'source', 'type', 'Species', 'Part', 'Grade', 'Price', 'Price_Per_Kg']
            for col, header in enumerate(headers_all):
                ws_all.write(0, col, header, fmt_header)
            for row_idx, data in enumerate(all_data_rows, start=1):
                ws_all.write(row_idx, 0, data['date'], fmt_text)
                ws_all.write(row_idx, 1, data['source'], fmt_text)
                ws_all.write(row_idx, 2, data['type'], fmt_text)
                ws_all.write(row_idx, 3, data['Species'], fmt_text)
                ws_all.write(row_idx, 4, data['Part'], fmt_text)
                ws_all.write(row_idx, 5, data['Grade'], fmt_text)
                ws_all.write(row_idx, 6, data['Price'], fmt_number)
                ws_all.write(row_idx, 7, data['Price_Per_Kg'], fmt_text)
            ws_all.set_column(0, 0, 12)
            ws_all.set_column(1, 1, 20)
            ws_all.set_column(2, 2, 12)
            ws_all.set_column(3, 3, 10)
            ws_all.set_column(4, 4, 12)
            ws_all.set_column(5, 5, 8)
            ws_all.set_column(6, 6, 12)
            ws_all.set_column(7, 7, 15)
            ws_all.freeze_panes(1, 0)

        wb.close()
        print(f"Excel 결과 저장: {filename}")

        all_data_filename = None
        if all_data_rows:
            all_data_filename = filename.replace('pivot', 'all_data')
            self.export_all_data(all_data_rows, all_data_filename)

        return filename, all_data_filename

    def export_all_data(self, all_data_rows, filename):
        """All_Data를 별도 Excel 파일로 저장"""
        wb = xlsxwriter.Workbook(filename)
        ws = wb.add_worksheet('All_Data')

        fmt_header = wb.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#fbfcfe'})
        fmt_text = wb.add_format({'border': 1})
        fmt_number = wb.add_format({'border': 1, 'num_format': '#,##0'})

        headers_all = ['date', 'source', 'type', 'Species', 'Part', 'Grade', 'Price', 'Price_Per_Kg']
        for col, header in enumerate(headers_all):
            ws.write(0, col, header, fmt_header)

        for row_idx, data in enumerate(all_data_rows, start=1):
            ws.write(row_idx, 0, data['date'], fmt_text)
            ws.write(row_idx, 1, data['source'], fmt_text)
            ws.write(row_idx, 2, data['type'], fmt_text)
            ws.write(row_idx, 3, data['Species'], fmt_text)
            ws.write(row_idx, 4, data['Part'], fmt_text)
            ws.write(row_idx, 5, data['Grade'], fmt_text)
            ws.write(row_idx, 6, data['Price'], fmt_number)
            ws.write(row_idx, 7, data['Price_Per_Kg'], fmt_text)

        ws.set_column(0, 0, 12)
        ws.set_column(1, 1, 20)
        ws.set_column(2, 2, 12)
        ws.set_column(3, 3, 10)
        ws.set_column(4, 4, 12)
        ws.set_column(5, 5, 8)
        ws.set_column(6, 6, 12)
        ws.set_column(7, 7, 15)
        ws.freeze_panes(1, 0)

        wb.close()
        print(f"All_Data Excel 저장: {filename}")
        return filename


# ============================================================
# 구글 드라이브 업로드 함수 (OAuth 방식)
# ============================================================

def upload_to_google_drive(file_path):
    """생성된 파일을 구글 드라이브에 업로드 (OAuth 방식, 원본 파일명 유지)"""
    try:
        token_json = os.environ.get('GDRIVE_TOKEN')
        folder_id = os.environ.get('GDRIVE_FOLDER_ID')

        if not token_json or not folder_id:
            print(f"[업로드 건너뜀] 환경변수 없음 (로컬 실행시 정상)")
            return

        token_data = json.loads(token_json)
        creds = Credentials.from_authorized_user_info(token_data)

        if creds.expired and creds.refresh_token:
            creds.refresh(Request())

        service = build('drive', 'v3', credentials=creds)

        file_metadata = {'name': os.path.basename(file_path), 'parents': [folder_id]}
        media = MediaFileUpload(file_path, resumable=True)

        print(f"구글 드라이브 업로드 중: {os.path.basename(file_path)}")
        result = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"업로드 완료: {os.path.basename(file_path)} (ID: {result.get('id')})")
    except Exception as e:
        print(f"업로드 실패: {os.path.basename(file_path)} - {e}")


async def main():
    print("=== 돼지 가격 수집 + 마진 계산 통합 프로그램 ===")
    service_key = os.getenv('EKAPE_API_KEY')
    if not service_key:
        try:
            with open('api_key.txt', 'r', encoding='utf-8') as f:
                service_key = f.read().strip()
        except FileNotFoundError: pass
    if not service_key:
        service_key = "LFq9u3tNGZKe+rUDioG7t8YJ6kLegDAwuy6sKuZAEHWUQ2RnPHUdh70zsjagYIdCWLKvoyxP4My/320pPvCatw=="

    # ── 1단계: 가격 데이터 수집 ──
    print("\n[1단계] 가격 데이터 수집 중...")
    scraper = PorkCompleteScraper(service_key=service_key)
    auction_success = scraper.collect_auction_data()
    market_success = await scraper.collect_pork_data()

    if not (auction_success or market_success):
        print("\n데이터 수집에 실패했습니다.")
        if scraper.errors:
            print(f"\n발생한 오류: {len(scraper.errors)}건")
            for error in scraper.errors[-3:]:
                print(f"  [{error['timestamp']}] {error['section']}: {error['error']}")
        return

    scraper.print_summary()

    price_filename = f"pork_wholesale_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    if not scraper.save_excel(price_filename):
        print("\n가격 파일 저장에 실패했습니다.")
        return

    # ── 2단계: 적수원가/마진 계산 (돼지는 등급 구분 없이 단일 그룹으로 계산) ──
    print("\n[2단계] 적수원가/마진 계산 중...")
    calculator = PorkMarginCalculatorCompare(price_filename)

    html_file = excel_file = all_data_file = None
    if calculator.load_data() and calculator.prepare_data() and calculator.generate_results():
        html_file = calculator.export_html()
        excel_file, all_data_file = calculator.export_excel()
    else:
        print("마진 계산에 실패했습니다. 가격 데이터만 업로드합니다.")

    # ── 3단계: 구글 드라이브 업로드 ──
    for f in [price_filename, html_file, excel_file, all_data_file]:
        if f:
            upload_to_google_drive(f)

    print(f"\n=== 모든 작업 완료 ===")
    print(f"가격 데이터: {price_filename}")
    if html_file:
        print(f"HTML: {html_file}")
    if excel_file:
        print(f"Excel (비교): {excel_file}")
    if all_data_file:
        print(f"Excel (All Data): {all_data_file}")

    if scraper.errors:
        print(f"\n발생한 오류: {len(scraper.errors)}건")
        for error in scraper.errors[-3:]:
            print(f"  [{error['timestamp']}] {error['section']}: {error['error']}")


if __name__ == "__main__":
    asyncio.run(main())
