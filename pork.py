#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
돼지고기 완전판 스크래핑 프로그램 (표시자 방식 + 재시도)
- 금천미트 부분육 시장가격 (16개 부위)
- 축산물품질평가원 도체 경락가격 (육질/육량등급별)
- Excel 파일로 통합 저장 + 구글 드라이브 업로드
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import traceback
import os
import requests
import xml.etree.ElementTree as ET

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


# ★ 추가 2/3: 구글 드라이브 업로드 함수
def upload_to_google_drive(file_path):
    """생성된 파일을 구글 드라이브에 업로드 (OAuth 방식)"""
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

        display_name = f"돼지가격_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        file_metadata = {'name': display_name, 'parents': [folder_id]}
        media = MediaFileUpload(file_path, resumable=True)

        print(f"구글 드라이브 업로드 중: {display_name}")
        result = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"업로드 완료! (파일ID: {result.get('id')})")
    except Exception as e:
        print(f"업로드 실패: {e}")


async def main():
    print("=== 돼지 도매가 + 경락가 스크래핑 프로그램 ===")
    service_key = os.getenv('EKAPE_API_KEY')
    if not service_key:
        try:
            with open('api_key.txt', 'r', encoding='utf-8') as f:
                service_key = f.read().strip()
        except FileNotFoundError: pass
    if not service_key:
        service_key = "LFq9u3tNGZKe+rUDioG7t8YJ6kLegDAwuy6sKuZAEHWUQ2RnPHUdh70zsjagYIdCWLKvoyxP4My/320pPvCatw=="
    
    scraper = PorkCompleteScraper(service_key=service_key)
    print("\n1. 도체 경락가 수집 중...")
    auction_success = scraper.collect_auction_data()
    print("\n2. 도매가 수집 중...")
    market_success = await scraper.collect_pork_data()
    
    # ★ 추가 3/3: 파일명 변수화 + 업로드 호출
    excel_filename = f"pork_wholesale_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    if auction_success or market_success:
        scraper.print_summary()
        excel_success = scraper.save_excel(excel_filename)
        if excel_success:
            upload_to_google_drive(excel_filename)
            print("\n모든 작업이 성공적으로 완료되었습니다!")
        else:
            print("\nExcel 저장에 실패했습니다.")
    else:
        print("\n데이터 수집에 실패했습니다.")
    
    if scraper.errors:
        print(f"\n발생한 오류: {len(scraper.errors)}건")
        for error in scraper.errors[-3:]:
            print(f"  [{error['timestamp']}] {error['section']}: {error['error']}")


if __name__ == "__main__":
    asyncio.run(main())
