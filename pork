#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
돼지고기 완전판 스크래핑 프로그램 (표시자 방식 + 재시도)
- 금천미트 부분육 시장가격 (16개 부위)
- 축산물품질평가원 도체 경락가격 (육질/육량등급별)
- Excel 파일로 통합 저장

[수집 방식]
- HTML 요소의 텍스트를 매칭하여 클릭하는 방식
- 실패한 부위는 자동으로 2회 재시도
"""

import asyncio
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import traceback
import os
import requests
import xml.etree.ElementTree as ET

class PorkCompleteScraper:
    def __init__(self, service_key=None):
        self.market_wholesale_data = []  # 금천미트 부분육 시장가격
        self.auction_data = []  # 도체 경락가격
        self.errors = []  # 에러 로그
        self.service_key = service_key
        self.session = requests.Session()
        self._setup_session()
    
    def _setup_session(self):
        """HTTP 세션 설정"""
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })
    
    def _get_element_text(self, element, tag, default=''):
        """XML 요소에서 텍스트 추출"""
        try:
            found = element.find(tag)
            return found.text.strip() if found is not None and found.text else default
        except:
            return default
        
    def log_error(self, section, error_msg):
        """에러 로깅"""
        error_entry = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'section': section,
            'error': str(error_msg)
        }
        self.errors.append(error_entry)
        print(f"[오류] {section}: {error_msg}")

    def collect_auction_data(self, target_date=None):
        """돼지 도체 경락가 수집 (API 전용)"""
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
        """돼지 경락가 수집 (API 방식) - inss.py 오류 수정 버전"""
        # 최대 30일간 이전 데이터 검색
        base_date = datetime.strptime(date_str, '%Y-%m-%d')

        # 정확한 API 파라미터 사용 (inss.py 오류 수정)
        api_endpoints = [
            {
                'url': "http://data.ekape.or.kr/openapi-data/service/user/grade/auct/pigGrade",
                'params_func': lambda date_api: {
                    'ServiceKey': self.service_key,  # 대문자 ServiceKey (inss.py에서 소문자로 잘못 사용)
                    'startYmd': date_api,
                    'endYmd': date_api,
                    'skinYn': 'Y',                   # Y: 탕박
                    'sexCd': '025003',               # 025003: 혼합(암수)
                    'egradeExceptYn': 'N'            # N: 등외제외
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
                    
                    # 결과 코드 확인
                    result_code = root.find('.//resultCode')
                    if result_code is not None and result_code.text in ['0000', '00']:
                        items = root.findall('.//item')
                        
                        if items:
                            print(f"    {api['name']} API에서 {len(items)}개 항목 발견")
                            collected = False
                            for item in items:
                                # 다양한 필드명 시도
                                grade_fields = ['gradeNm', 'gradeName', 'grade']
                                # 제주제외 전국 가격을 우선적으로 확인
                                price_fields = ['c_1101eTotAmt', 'CTotAmt', 'auctAmt', 'price', 'avgPrice']
                                
                                grade_nm = None
                                for field in grade_fields:
                                    grade_nm = self._get_element_text(item, field)
                                    if grade_nm:
                                        break
                                if not grade_nm:
                                    grade_nm = '전체'
                                
                                # 제주제외 전국 가격 우선 사용 (c_1101eTotAmt)
                                price_value = None
                                # c_1101eTotAmt: 제주제외 전국 평균가격 (우선순위 1)
                                price_str = self._get_element_text(item, 'c_1101eTotAmt')
                                if price_str and price_str != '0':
                                    try:
                                        price_value = int(price_str.replace(',', ''))
                                    except ValueError:
                                        pass

                                # 제주제외 가격이 없으면 다른 가격 필드 시도
                                if not price_value:
                                    for field in price_fields:
                                        price_str = self._get_element_text(item, field)
                                        if price_str and price_str != '0':
                                            try:
                                                price_value = int(price_str.replace(',', ''))
                                                if price_value > 0:
                                                    break
                                            except ValueError:
                                                continue
                                
                                # 디버깅: 원본 데이터와 두수 정보 출력
                                if grade_nm and price_value:
                                    # 두수/물량 관련 필드 확인 (제주제외 두수 우선)
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
                                            except ValueError:
                                                continue
                                    
                                    # 제주제외 여부 표시
                                    jeju_excluded = "제주제외" if used_field == 'c_1101eTotCnt' else "제주포함"
                                    price_type = "제주제외" if 'c_1101eTotAmt' in str(price_value) else "제주포함"
                                    
                                    if quantity_value:
                                        print(f"      원본 데이터: 등급='{grade_nm}', 가격={price_value:,}원({price_type}), 두수={quantity_value:,}두({jeju_excluded})")
                                    else:
                                        print(f"      원본 데이터: 등급='{grade_nm}', 가격={price_value:,}원({price_type}), 두수=미확인")
                                
                                if price_value and price_value > 0:
                                    # 제주제외 두수 정보 우선 수집
                                    quantity_value = 0
                                    quantity_source = "미확인"
                                    # c_1101eTotCnt: 제주제외 전국 두수 (우선순위 1)
                                    qty_str = self._get_element_text(item, 'c_1101eTotCnt')
                                    if qty_str and qty_str != '0':
                                        try:
                                            quantity_value = int(qty_str.replace(',', ''))
                                            quantity_source = "제주제외"
                                        except ValueError:
                                            pass

                                    # 제주제외 두수가 없으면 전체 두수 사용
                                    if quantity_value == 0:
                                        qty_str = self._get_element_text(item, 'CTotCnt')
                                        if qty_str and qty_str != '0':
                                            try:
                                                quantity_value = int(qty_str.replace(',', ''))
                                                quantity_source = "제주포함"
                                            except ValueError:
                                                pass
                                    
                                    # 등급 단순화 - 정확한 등급명 보존
                                    grade_simplified = grade_nm
                                    if '등외제외' in grade_nm:
                                        grade_simplified = '등외제외'
                                    elif '1+' in grade_nm and '1++' not in grade_nm:
                                        grade_simplified = '1+'
                                    elif grade_nm.startswith('1') and '+' not in grade_nm:
                                        grade_simplified = '1'
                                    elif grade_nm.startswith('2'):
                                        grade_simplified = '2'
                                    elif '등외' in grade_nm or 'E' in grade_nm:
                                        grade_simplified = '등외'
                                    
                                    # 유효한 등급만 저장 (등외제외 추가)
                                    valid_grades = ['1+', '1', '2', '등외', '등외제외']
                                    if grade_simplified in valid_grades:
                                        # 가격 소스 확인 (제주제외 여부를 정확히 판단)
                                        price_source = "제주제외전국" if self._get_element_text(item, 'c_1101eTotAmt') else "제주포함전국"
                                        
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
        """돼지 도매가 수집 (웹스크래핑 + API 통합 방식)"""
        print("=== 돼지 도매가 수집 시작 ===")
        
        # 1. API 도매가 수집 시도
        print("1. API 도매가 수집 중...")
        api_success = self.collect_pork_wholesale_data_api()
        
        # 2. 웹스크래핑 시장가 수집
        print("2. 웹스크래핑 시장가 수집 중...")
        web_success = False
        try:
            async with async_playwright() as p:
                browser = await p.firefox.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-dev-shm-usage']
                )
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                page = await context.new_page()
                page.set_default_timeout(60000)
                
                try:
                    web_success = await asyncio.wait_for(
                        self._collect_pork_market_data(page), 
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    print("돼지 웹스크래핑 시간 초과")
                except Exception as e:
                    print(f"돼지 웹스크래핑 오류: {e}")
                
                await browser.close()
                    
        except Exception as e:
            self.log_error("돼지수집", f"웹스크래핑 실패: {e}")
            traceback.print_exc()
        
        # 결과 확인
        if api_success or web_success or len(self.market_wholesale_data) > 0:
            total_count = len(self.market_wholesale_data)
            print(f"돼지 데이터 수집 완료: 시장가 {total_count}건")
            return True
        else:
            print("API와 웹스크래핑 모두 실패 - 임시 데이터 생성")
            return self._generate_fallback_data()

    def collect_pork_wholesale_data_api(self, target_date=None):
        """돼지 공식 도매가 수집 (API 방식) - inss.py에서 가져옴"""
        if not self.service_key:
            print("API 인증키가 없어 도매가 API 수집 건너뜀")
            return False
            
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 최대 7일간 이전 데이터 검색
        base_date = datetime.strptime(target_date, '%Y-%m-%d')
        
        for days_back in range(8):
            try_date = base_date - timedelta(days=days_back)
            try_date_str = try_date.strftime('%Y-%m-%d')
            date_api = try_date_str.replace('-', '')
            
            url = "http://data.ekape.or.kr/openapi-data/service/user/grade/auct/pigJejuGrade"
            
            params = {
                'ServiceKey': self.service_key,
                'delngDe': date_api
            }
            
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
                                        'date': try_date_str,
                                        'source': '축산물품질평가원(API)',
                                        'type': '공식도매가',
                                        '축종': '돼지',
                                        '부위': '전체',
                                        '등급': grade_name,
                                        '가격': price_value,
                                        'kg당가격': f"{price_value}원"
                                    })
                                    print(f"    돼지 공식도매가 API: {grade_name}, {price_value:,}원")
                                except ValueError:
                                    continue
                        
                        print(f"돼지 공식도매가 API 수집 성공: {try_date_str} ({len(items)}건)")
                        return True
                
                print(f"돼지 공식도매가 API {try_date_str}: 데이터 없음, 이전 날짜 시도...")
                
            except Exception as e:
                print(f"돼지 공식도매가 API {try_date_str} 오류: {str(e)}")
                continue
        
        print("돼지 공식도매가 API: 8일간 데이터를 찾을 수 없음")
        return False

    async def _collect_pork_market_data(self, page):
        """돼지 시장가 수집 (표시자 방식 + 재시도)"""
        try:
            # 정확한 부위명만 수집 (완전 일치)
            all_parts = [
                "미박삼겹",      # 삼겹 중 정확히 "미박삼겹"만
                "등심",          # 등심 중 정확히 "등심"만
                "목심",          # 목심 중 정확히 "목심"만
                "안심",          # 안심 중 정확히 "안심"만
                "미박앞다리",    # 앞다리 중 "미박앞다리"만
                "미박뒷다리",    # 뒷다리 중 "미박뒷다리"만
                "등갈비",        # 갈비 관련
                "갈비",          # 갈비 관련
                "등심덧살",      # 덧살 관련
                "갈매기",        # 갈매기
                "항정",          # 항정
                "미박앞사태",    # 사태 평균 계산용
                "미박뒷사태",    # 사태 평균 계산용
                "냉동등뼈",      # 냉동 부위
                "냉동지방A",     # 냉동 부위
                "냉동잡육A",     # 냉동 부위
                "냉동앞장족",    # 장족 평균 계산용
                "냉동뒷장족",    # 장족 평균 계산용
                "냉동덜미살",    # 냉동 부위
                "냉동막창",      # 냉동 부위
                "냉동돈두롤"     # 냉동 부위
            ]

            # 3번 시도 (초기 + 재시도 2회)
            failed_parts = all_parts.copy()

            for attempt in range(1, 4):
                if not failed_parts:
                    break

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
                        # 부위명으로 클릭 (완전 일치만)
                        click_result = await page.evaluate(f"""
                            (partName) => {{
                                const listItems = document.querySelectorAll('li.ctg-item');
                                for (let li of listItems) {{
                                    const categoryP = li.querySelector('p.category');
                                    if (categoryP && categoryP.textContent) {{
                                        const text = categoryP.textContent.trim().replace(/\\s*\\(\\d+\\)\\s*$/, '').trim();
                                        if (text === partName) {{
                                            const link = li.querySelector('a.ctg-link');
                                            if (link) {{
                                                link.scrollIntoView({{ block: 'center' }});
                                                link.click();
                                                return {{
                                                    success: true,
                                                    matched: text,
                                                    original: categoryP.textContent.trim()
                                                }};
                                            }}
                                        }}
                                    }}
                                }}
                                return {{ success: false }};
                            }}
                        """, part)

                        clicked = click_result.get('success', False)
                        if clicked:
                            matched_text = click_result.get('matched', '')
                            original_text = click_result.get('original', '')
                            print(f"    ✓ 클릭: '{matched_text}' (원본: {original_text})")

                        if not clicked:
                            print(f"    ✗ 부위를 찾을 수 없음")
                            failed_parts.append(part)
                            continue

                        await page.wait_for_load_state('domcontentloaded', timeout=10000)

                        # 충분한 대기 시간 (Vue.js 렌더링)
                        print(f"    페이지 렌더링 대기 중...")
                        await page.wait_for_timeout(3000)

                        # 상품 목록 또는 품절 메시지 확인 (최대 15초)
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

                                    return {
                                        priceCount: prices.length,
                                        hasContent: prices.length > 0,
                                        hasSoldout: soldoutWrap !== null || soldoutMsg
                                    };
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
                            if soldout_found:
                                print(f"    품절 상품 - 최종 판매가 확인 중...")
                            else:
                                print(f"    재고 없음 - 최종 판매가 확인 중...")

                            # 재고 없을 때 최종 판매가 찾기
                            last_price = await page.evaluate("""
                                () => {
                                    // 패턴 1: .soldout-wrap 안의 최종 판매가
                                    const soldoutWrap = document.querySelector('.soldout-wrap');
                                    if (soldoutWrap) {
                                        const priceRow = soldoutWrap.querySelector('dl.row.price');
                                        if (priceRow) {
                                            const priceEl = priceRow.querySelector('.pd-price.c-primary');
                                            if (priceEl) {
                                                const match = priceEl.textContent.match(/([0-9,]+)/);
                                                if (match) {
                                                    const price = parseInt(match[1].replace(/,/g, ''));
                                                    if (price >= 100 && price <= 100000) {
                                                        console.log('Found soldout price:', price);
                                                        return price;
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // 패턴 2: "최종 판매가" 텍스트로 찾기
                                    const allText = document.body.innerText;
                                    const lastPriceMatch = allText.match(/최종\\s*판매가[^0-9]*([0-9,]+)/i);
                                    if (lastPriceMatch) {
                                        const price = parseInt(lastPriceMatch[1].replace(/,/g, ''));
                                        if (price >= 100 && price <= 100000) {
                                            console.log('Found last price in text:', price);
                                            return price;
                                        }
                                    }

                                    return null;
                                }
                            """)

                            if last_price:
                                print(f"    ✓ 마지막 판매가: {last_price:,}원/kg")
                                price = last_price
                                # 데이터 저장으로 넘어감
                            else:
                                print(f"    ✗ 마지막 판매가도 찾을 수 없음")
                                # 스크린샷 저장
                                await page.screenshot(path=f"error_{part}.png")
                                print(f"    디버그 스크린샷: error_{part}.png")
                                failed_parts.append(part)
                                continue
                        else:
                            # 정상적으로 상품 목록이 있는 경우
                            await page.wait_for_timeout(1000)

                            # 정렬 전 가격 저장
                            old_price = await page.evaluate("""
                                () => {
                                    const firstProduct = document.querySelector('.product-unit, .d-flex');
                                    if (firstProduct) {
                                        const priceEl = firstProduct.querySelector('.pd-price.xs.c-primary');
                                        return priceEl ? priceEl.textContent.trim() : null;
                                    }
                                    return null;
                                }
                            """)

                            # 정렬
                            sort_clicked = await page.evaluate("""
                                () => {
                                    const buttons = document.querySelectorAll('button');
                                    for (let btn of buttons) {
                                        if (btn.textContent && btn.textContent.includes('Kg당') && btn.textContent.includes('낮은')) {
                                            btn.click();
                                            return true;
                                        }
                                    }
                                    return false;
                                }
                            """)

                            if sort_clicked:
                                print(f"    정렬 버튼 클릭 (정렬 전: {old_price})")
                                # 정렬 완료 대기
                                for wait_count in range(10):
                                    await page.wait_for_timeout(500)
                                    new_price = await page.evaluate("""
                                        () => {
                                            const firstProduct = document.querySelector('.product-unit, .d-flex');
                                            if (firstProduct) {
                                                const priceEl = firstProduct.querySelector('.pd-price.xs.c-primary');
                                                return priceEl ? priceEl.textContent.trim() : null;
                                            }
                                            return null;
                                        }
                                    """)
                                    if new_price and new_price != old_price:
                                        print(f"    정렬 완료 ({(wait_count+1)*0.5}초, 정렬 후: {new_price})")
                                        break

                                await page.wait_for_timeout(1000)
                            else:
                                print(f"    경고: 정렬 버튼 없음")
                                await page.wait_for_timeout(1500)

                            # 가격 수집
                            price = await page.evaluate("""
                                () => {
                                    const firstProduct = document.querySelector('.product-unit, .d-flex');
                                    if (firstProduct) {
                                        const priceEl = firstProduct.querySelector('.pd-price.xs.c-primary');
                                        if (priceEl) {
                                            const match = priceEl.textContent.match(/([0-9,]+)/);
                                            if (match) {
                                                const price = parseInt(match[1].replace(/,/g, ''));
                                                if (price >= 1000 && price <= 100000) {
                                                    return price;
                                                }
                                            }
                                        }
                                    }
                                    return null;
                                }
                            """)

                        if price:
                            self.market_wholesale_data.append({
                                'date': datetime.now().strftime('%Y-%m-%d'),
                                'source': '금천미트',
                                'type': '시장도매가',
                                '축종': '돼지',
                                '부위': part,
                                '등급': '1등급',
                                '가격': price,
                                'kg당가격': f"{price:,}원"
                            })
                            print(f"    ✓ 가격: {price:,}원/kg")
                            success = True
                        else:
                            print(f"    ✗ 가격을 찾을 수 없음")

                        # 다음 부위를 위해 초기 페이지로
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

            # 사태 평균 계산 (미박앞사태 + 미박뒷사태) / 2
            self._calculate_satae_average()

            # 장족 평균 계산 (냉동앞장족 + 냉동뒷장족) / 2
            self._calculate_jangjok_average()

            return len(self.market_wholesale_data) > 0
            
        except Exception as e:
            self.log_error("돼지수집", f"오류: {e}")
            return False

    def _calculate_satae_average(self):
        """사태 평균 계산: (미박앞사태 + 미박뒷사태) / 2"""
        print("\n=== 사태 평균 계산 ===")

        # 미박앞사태와 미박뒷사태 가격 찾기
        front_satae = None
        back_satae = None

        for data in self.market_wholesale_data:
            if data['부위'] == '미박앞사태':
                front_satae = data['가격']
                print(f"미박앞사태 가격: {front_satae:,}원/kg")
            elif data['부위'] == '미박뒷사태':
                back_satae = data['가격']
                print(f"미박뒷사태 가격: {back_satae:,}원/kg")

        # 둘 다 있으면 평균 계산
        if front_satae and back_satae:
            avg_price = int((front_satae + back_satae) / 2)

            # 사태 평균 데이터 추가
            self.market_wholesale_data.append({
                'date': datetime.now().strftime('%Y-%m-%d'),
                'source': '금천미트',
                'type': '시장도매가',
                '축종': '돼지',
                '부위': '사태',
                '등급': '1등급',
                '가격': avg_price,
                'kg당가격': f"{avg_price:,}원"
            })
            print(f"사태 평균 가격: {avg_price:,}원/kg (= ({front_satae:,} + {back_satae:,}) / 2)")
        else:
            missing = []
            if not front_satae:
                missing.append('미박앞사태')
            if not back_satae:
                missing.append('미박뒷사태')
            print(f"사태 평균 계산 실패: {', '.join(missing)} 데이터 없음")

    def _calculate_jangjok_average(self):
        """장족 평균 계산: (냉동앞장족 + 냉동뒷장족) / 2"""
        print("\n=== 장족 평균 계산 ===")

        # 냉동앞장족과 냉동뒷장족 가격 찾기
        front_jangjok = None
        back_jangjok = None

        for data in self.market_wholesale_data:
            if data['부위'] == '냉동앞장족':
                front_jangjok = data['가격']
                print(f"냉동앞장족 가격: {front_jangjok:,}원/kg")
            elif data['부위'] == '냉동뒷장족':
                back_jangjok = data['가격']
                print(f"냉동뒷장족 가격: {back_jangjok:,}원/kg")

        # 둘 다 있으면 평균 계산
        if front_jangjok and back_jangjok:
            avg_price = int((front_jangjok + back_jangjok) / 2)

            # 장족 평균 데이터 추가
            self.market_wholesale_data.append({
                'date': datetime.now().strftime('%Y-%m-%d'),
                'source': '금천미트',
                'type': '시장도매가',
                '축종': '돼지',
                '부위': '장족',
                '등급': '1등급',
                '가격': avg_price,
                'kg당가격': f"{avg_price:,}원"
            })
            print(f"장족 평균 가격: {avg_price:,}원/kg (= ({front_jangjok:,} + {back_jangjok:,}) / 2)")
        else:
            missing = []
            if not front_jangjok:
                missing.append('냉동앞장족')
            if not back_jangjok:
                missing.append('냉동뒷장족')
            print(f"장족 평균 계산 실패: {', '.join(missing)} 데이터 없음")

    def _generate_fallback_data(self):
        """웹스크래핑 실패시 - 임시 가격 생성 금지, 정확한 데이터 수집만 수행"""
        print("임시 가격 사용 금지 - 정확한 웹스크래핑 데이터만 수집됨")
        self.log_error("돼지_웹스크래핑", "웹스크래핑 데이터 없음 - 임시 가격을 사용하지 않음")
        return False

    def _clean_data_for_excel(self, data_list):
        """Excel 저장을 위해 데이터 정리"""
        cleaned_data = []
        for item in data_list:
            cleaned_item = {}
            for key, value in item.items():
                # 한글 키를 영어로 변환
                if key == '축종':
                    cleaned_item['Species'] = value
                elif key == '부위':
                    cleaned_item['Part'] = value  
                elif key == '등급':
                    cleaned_item['Grade'] = value
                elif key == '가격':
                    cleaned_item['Price'] = value
                elif key == 'kg당가격':
                    cleaned_item['Price_Per_Kg'] = value
                else:
                    cleaned_item[key] = value
            cleaned_data.append(cleaned_item)
        return cleaned_data

    def save_excel(self, filename=None):
        """Excel 파일 저장"""
        if filename is None:
            filename = f"pork_wholesale_prices_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        try:
            if not self.market_wholesale_data and not self.auction_data:
                print("저장할 데이터가 없습니다.")
                return False
            
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # 모든 데이터를 하나의 시트에 통합
                all_data = []
                
                # 부분육 시장가격 데이터 추가
                if self.market_wholesale_data:
                    all_data.extend(self.market_wholesale_data)
                
                # 도체 경락가격 데이터 추가
                if self.auction_data:
                    all_data.extend(self.auction_data)
                
                # 통합 데이터를 하나의 시트에 저장
                if all_data:
                    df = pd.DataFrame(all_data)

                    # type 정렬 순서: 도체경락가 → 시장도매가
                    type_order = ['도체경락가', '시장도매가', '공식도매가']

                    # 부위 정렬 순서
                    part_order = [
                        '미박삼겹', '등심', '목심', '안심',
                        '미박앞다리', '미박뒷다리',
                        '등갈비', '갈비',
                        '등심덧살', '갈매기', '항정',
                        '미박앞사태', '미박뒷사태', '사태',
                        '냉동등뼈', '냉동지방A', '냉동잡육A', '냉동돈피',
                        '냉동앞장족', '냉동뒷장족', '장족',
                        '냉동덜미살', '냉동막창', '냉동돈두롤',
                        '전체'  # API 공식도매가
                    ]

                    # Categorical로 변환하여 순서 지정
                    df['type'] = pd.Categorical(df['type'], categories=type_order, ordered=True)
                    df['부위'] = pd.Categorical(df['부위'], categories=part_order, ordered=True)

                    # 정렬: type 먼저, 그 다음 부위
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

    def _create_summary_sheet(self, writer):
        """종합 분석 시트 생성"""
        try:
            summary_data = []
            
            # 도체 경락가 요약
            if self.auction_data:
                auction_df = pd.DataFrame(self.auction_data)
                summary_data.append(['=== 도체 경락가 (생체 전체) ===', '', '', ''])
                for _, row in auction_df.iterrows():
                    summary_data.append([
                        '도체경락가', 
                        row['등급'] + '등급', 
                        f"{row['가격']:,}원/kg",
                        row['date']
                    ])
                summary_data.append(['', '', '', ''])
            
            # 부분육 시장가 요약 (등급별 평균)
            if self.market_wholesale_data:
                market_df = pd.DataFrame(self.market_wholesale_data)
                summary_data.append(['=== 부분육 시장가격 (등급별 평균) ===', '', '', ''])
                
                for grade in ['1+', '1', '2', '등외']:
                    grade_data = market_df[market_df['등급'] == grade]
                    if not grade_data.empty:
                        avg_price = int(grade_data['가격'].mean())
                        summary_data.append([
                            '부분육평균',
                            grade + '등급',
                            f"{avg_price:,}원/kg",
                            grade_data.iloc[0]['date']
                        ])
                summary_data.append(['', '', '', ''])
                
                # 부위별 최고/최저 가격
                summary_data.append(['=== 부위별 가격 범위 (1+등급 기준) ===', '', '', ''])
                grade_1p = market_df[market_df['등급'] == '1+']
                if not grade_1p.empty:
                    max_part = grade_1p.loc[grade_1p['가격'].idxmax()]
                    min_part = grade_1p.loc[grade_1p['가격'].idxmin()]
                    
                    summary_data.append([
                        '최고가부위', 
                        max_part['부위'], 
                        f"{max_part['가격']:,}원/kg",
                        max_part['date']
                    ])
                    summary_data.append([
                        '최저가부위', 
                        min_part['부위'], 
                        f"{min_part['가격']:,}원/kg", 
                        min_part['date']
                    ])
            
            # DataFrame으로 변환하여 저장
            if summary_data:
                summary_df = pd.DataFrame(summary_data, columns=['Type', 'Item', 'Price', 'Date'])
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
        except Exception as e:
            print(f"종합 분석 시트 생성 오류: {e}")

    def print_summary(self):
        """수집 결과 요약 출력"""
        if not self.market_wholesale_data and not self.auction_data:
            print("수집된 데이터가 없습니다.")
            return
        
        print(f"\n=== 돼지 데이터 수집 결과 요약 ===")
        
        # 도체 경락가 요약
        if self.auction_data:
            print(f"도체 경락가: {len(self.auction_data)}건")
            for data in self.auction_data:
                print(f"  - {data['등급']}등급: {data['가격']:,}원/kg")
        
        # 도매가 요약
        if self.market_wholesale_data:
            df = pd.DataFrame(self.market_wholesale_data)
            print(f"도매가: {len(df)}건")
            print(f"수집 부위: {df['부위'].nunique()}개")
            
            for part in sorted(df['부위'].unique()):
                part_df = df[df['부위'] == part]
                avg_price = part_df['가격'].mean()
                print(f"  - {part}: {avg_price:,.0f}원/kg")

async def main():
    """메인 실행 함수"""
    print("=== 돼지 도매가 + 경락가 스크래핑 프로그램 ===")
    
    # API 키 자동 로드
    service_key = None
    
    # 1. 환경변수에서 API 키 확인
    service_key = os.getenv('EKAPE_API_KEY')
    
    # 2. api_key.txt 파일에서 확인
    if not service_key:
        try:
            with open('api_key.txt', 'r', encoding='utf-8') as f:
                service_key = f.read().strip()
        except FileNotFoundError:
            pass
    
    # 3. 하드코딩된 키
    if not service_key:
        service_key = "LFq9u3tNGZKe+rUDioG7t8YJ6kLegDAwuy6sKuZAEHWUQ2RnPHUdh70zsjagYIdCWLKvoyxP4My/320pPvCatw=="
    
    scraper = PorkCompleteScraper(service_key=service_key)
    
    # 1. 도체 경락가 수집 (API)
    print("\n1. 도체 경락가 수집 중...")
    auction_success = scraper.collect_auction_data()
    
    # 2. 도매가 수집 (웹스크래핑) - 기존 방식
    print("\n2. 도매가 수집 중...")
    market_success = await scraper.collect_pork_data()
    
    if auction_success or market_success:
        # 결과 요약 출력
        scraper.print_summary()
        
        # Excel 파일 저장
        excel_success = scraper.save_excel()
        
        if excel_success:
            print("\n모든 작업이 성공적으로 완료되었습니다!")
        else:
            print("\nExcel 저장에 실패했습니다.")
    else:
        print("\n데이터 수집에 실패했습니다.")
    
    # 오류 로그 출력
    if scraper.errors:
        print(f"\n발생한 오류: {len(scraper.errors)}건")
        for error in scraper.errors[-3:]:
            print(f"  [{error['timestamp']}] {error['section']}: {error['error']}")

    # 프로그램 종료 전 사용자 입력 대기
    print("\n프로그램을 종료하려면 엔터를 누르세요...")
    input()

if __name__ == "__main__":
    asyncio.run(main())
