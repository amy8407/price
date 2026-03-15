#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
한우 가격 수집 + 마진 계산 통합 프로그램
- 금천미트 부분육 시장가격 수집 (BeefCompleteScraper)
- 축산물품질평가원 도체 경락가격 수집 (BeefCompleteScraper)
- 적수원가/마진 계산 비교 (MarginCalculatorCompare)
"""

import asyncio
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import traceback
import requests
import xml.etree.ElementTree as ET
import xlsxwriter
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


# ============================================================
# BeefCompleteScraper 클래스 (beef_selector_clean.py 원본)
# ============================================================

class BeefCompleteScraper:
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
        """한우 도체 경락가 수집 (API 전용)"""
        print("=== 한우 도체 경락가 수집 시작 ===")

        if not self.service_key:
            self.log_error("경락가", "API 인증키가 필요합니다")
            return False

        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        try:
            beef_success = self._collect_beef_auction_data_api(target_date)

            if beef_success:
                print(f"한우 도체 경락가 수집 완료: {len(self.auction_data)}건")
                return True
            else:
                return False

        except Exception as e:
            self.log_error("경락가", f"전체 수집 실패: {e}")
            return False

    def _collect_beef_auction_data_api(self, date_str):
        """한우 도체 경락가 수집 - 육질등급만 수집"""
        base_date = datetime.strptime(date_str, '%Y-%m-%d')

        for days_back in range(30):
            try_date = base_date - timedelta(days=days_back)
            try_date_str = try_date.strftime('%Y-%m-%d')
            date_api = try_date_str.replace('-', '')

            url = "http://data.ekape.or.kr/openapi-data/service/user/grade/auct/cattle"

            params = {
                'ServiceKey': self.service_key,
                'startYmd': date_api,
                'endYmd': date_api,
                'breedCd': '024001',  # 한우
                'sexCd': '025001',    # 거세
                'qgradeYn': 'Y',      # 육질등급 포함
                'defectIncludeYn': 'N'  # 결함축 제외
            }

            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()

                root = ET.fromstring(response.text)
                result_code = root.find('.//resultCode')

                if result_code is not None and result_code.text in ['0000', '00']:
                    items = root.findall('.//item')

                    if items:
                        # 육질등급 1++, 1+, 1, 2만 수집
                        target_grades = ['1++', '1+', '1', '2']
                        collected = False

                        print(f"    [디버그] API에서 받은 전체 항목 수: {len(items)}개")

                        for item in items:
                            grade_nm = self._get_element_text(item, 'gradeNm', '미분류')
                            ctot_amt = self._get_element_text(item, 'CTotAmt', '0')

                            # 디버깅: 모든 등급명 출력
                            print(f"    [디버그] API 등급명: '{grade_nm}', 가격: '{ctot_amt}'")

                            if ctot_amt != '0' and ctot_amt != '' and int(ctot_amt.replace(',', '')) > 0:
                                try:
                                    price_value = int(ctot_amt.replace(',', ''))

                                    # 육질등급만 필터링
                                    grade_simplified = None
                                    if '1++' in grade_nm:
                                        grade_simplified = '1++'
                                    elif '1+' in grade_nm:
                                        grade_simplified = '1+'
                                    elif grade_nm.startswith('1') and not grade_nm.startswith('1+'):
                                        grade_simplified = '1'
                                    elif grade_nm.startswith('2'):
                                        grade_simplified = '2'

                                    # 디버깅: 변환 결과 출력
                                    print(f"    [디버그] '{grade_nm}' → '{grade_simplified}'")

                                    # 육질등급만 저장
                                    if grade_simplified in target_grades:
                                        self.auction_data.append({
                                            'date': try_date_str,
                                            'source': '축산물품질평가원',
                                            'type': '도체경락가',
                                            '축종': '한우',
                                            '부위': '전체',
                                            '등급': grade_simplified,
                                            'grade_detail': grade_nm,
                                            '가격': price_value,
                                            'kg당가격': f"{price_value:,}원"
                                        })
                                        print(f"    [O] 수집: {grade_simplified}등급 ({grade_nm}), {price_value:,}원")
                                        collected = True
                                    else:
                                        print(f"    [X] 제외: '{grade_simplified}' (대상 등급 아님)")

                                except Exception as e:
                                    print(f"    [오류] 가격 파싱 실패: {grade_nm}, {e}")

                        if collected:
                            print(f"한우 도체 경락가 수집 완료: {try_date_str}")
                            return True

                print(f"한우 도체 경락가 {try_date_str}: 데이터 없음, 이전 날짜 시도...")

            except Exception as e:
                print(f"한우 도체 경락가 {try_date_str} 오류: {str(e)}")
                continue

        self.log_error("한우경락가API", "30일간 경락가 데이터를 찾을 수 없음")
        return False

    async def collect_market_wholesale_data(self, timeout=600):
        """금천미트 부분육 시장가격 수집"""
        print("=== 금천미트 부분육 시장가격 수집 시작 ===")

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
                    beef_success = await asyncio.wait_for(
                        self._collect_beef_market_data(page),
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    print("한우 웹스크래핑 시간 초과")
                    beef_success = False
                except Exception as e:
                    print(f"한우 웹스크래핑 오류: {e}")
                    beef_success = False

                await browser.close()

                if beef_success:
                    beef_count = len(self.market_wholesale_data)
                    print(f"금천미트 부분육 가격 수집 완료: {beef_count}건")
                    return True
                else:
                    print("웹스크래핑 실패 - 데이터 수집 실패")
                    return False

        except Exception as e:
            self.log_error("부분육가격", f"수집 실패: {e}")
            traceback.print_exc()
            return False

    async def _collect_beef_market_data(self, page):
        """한우 부분육 가격 수집 (표시자 방식 + 재시도)"""
        try:
            # 정확한 부위명만 수집 (완전 일치)
            all_parts = [
                "안심",
                "등심",
                "채끝",
                "부채살",
                "앞다리살",
                "업진살",
                "치마살",
                "제비추리",
                "토시살",
                "안창살",
                "목심",
                "우둔살",
                "설도",
                "양지머리외",
                "사태",
                "갈비",
                "차돌박이"
            ]

            grades = ["1++", "1+", "1", "2"]

            # 뼈류 부위 - 1등급만 조회 (내부명칭: 조회명칭)
            bone_parts = {
                "사골": "사골",
                "꼬리": "꼬리반골",
                "잡뼈": "잡뼈",
                "우족": "우족",
                "도가니": "도가니",
                "스지": "스지"
            }

            # 3번 시도 (초기 + 재시도 2회)
            failed_parts = all_parts.copy()

            for attempt in range(1, 4):
                if not failed_parts:
                    break

                if attempt > 1:
                    print(f"\n=== {attempt}차 재시도: {len(failed_parts)}개 부위 ===")

                parts_to_try = failed_parts.copy()
                failed_parts = []

                beef_url = "https://www.ekcm.co.kr/pd/product?dispCtgNo=14&dispCtgNm=%EA%B5%AD%EB%82%B4%EC%82%B0+%ED%95%9C%EC%9A%B0+%EC%95%94%EC%86%8C"
                print(f"    페이지 로딩 중: {beef_url}")
                await page.goto(beef_url, wait_until='networkidle', timeout=60000)

                # 더 긴 시간 대기 및 여러 선택자 시도
                page_loaded = False
                for wait_attempt in range(6):  # 30초 동안 시도
                    try:
                        await page.wait_for_selector('li.ctg-item', timeout=5000)
                        page_loaded = True
                        print(f"    카테고리 목록 로드 완료 ({(wait_attempt+1)*5}초)")
                        break
                    except:
                        print(f"    카테고리 로딩 대기 중... ({(wait_attempt+1)*5}초)")
                        await page.wait_for_timeout(1000)

                if not page_loaded:
                    print(f"    [경고] 카테고리 목록을 찾을 수 없지만 계속 진행")

                await page.wait_for_timeout(3000)

                for i, part in enumerate(parts_to_try, 1):
                    print(f"[한우 {i}/{len(parts_to_try)}] {part} 부위 수집 중...")
                    part_success = False

                    try:
                        # 부위명으로 클릭 (완전 일치만)
                        click_result = await page.evaluate(f"""
                            (partName) => {{
                                const listItems = document.querySelectorAll('li.ctg-item');
                                for (let li of listItems) {{
                                    const categoryP = li.querySelector('p.category');
                                    if (categoryP && categoryP.textContent) {{
                                        const text = categoryP.textContent.trim().replace(/\\s*\\(\\d+\\)\\s*$/, '').trim();
                                        // 정확히 일치하는 경우만 (앞다리살은 "앞다리살(앞다리+꾸리)" 제외)
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
                            print(f"    [O] 클릭: '{matched_text}' (원본: {original_text})")

                        if not clicked:
                            print(f"    [X] 부위를 찾을 수 없음")
                            failed_parts.append(part)
                            continue

                        await page.wait_for_load_state('domcontentloaded', timeout=10000)

                        # 충분한 대기 시간 (Vue.js 렌더링)
                        print(f"    페이지 렌더링 대기 중...")
                        await page.wait_for_timeout(3000)

                        # 상품 목록 확인 (더 다양한 선택자 시도)
                        print(f"    상품 확인 중...")
                        product_found = False
                        soldout_found = False

                        for wait_check in range(30):
                            await page.wait_for_timeout(500)
                            check_result = await page.evaluate("""
                                () => {
                                    // 다양한 가격 선택자 시도
                                    const prices1 = document.querySelectorAll('.pd-price.xs.c-primary');
                                    const prices2 = document.querySelectorAll('.pd-price');
                                    const prices3 = document.querySelectorAll('.price');
                                    const totalPrices = prices1.length + prices2.length + prices3.length;

                                    const soldoutWrap = document.querySelector('.soldout-wrap');
                                    const soldoutMsg = document.body.innerText.includes('상품이 모두 판매되었습니다');

                                    return {
                                        priceCount: totalPrices,
                                        hasContent: totalPrices > 0,
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
                                                    if (price >= 10000 && price <= 1000000) {
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
                                        if (price >= 10000 && price <= 1000000) {
                                            console.log('Found last price in text:', price);
                                            return price;
                                        }
                                    }

                                    return null;
                                }
                            """)

                            if last_price:
                                print(f"    [O] 최종 판매가 발견: {last_price:,}원/kg")
                                # 등급별로 최종 판매가 저장
                                for grade in grades:
                                    self.market_wholesale_data.append({
                                        'date': datetime.now().strftime('%Y-%m-%d'),
                                        'source': '금천미트',
                                        'type': '부분육시장가',
                                        '축종': '한우',
                                        '부위': part,
                                        '등급': grade,
                                        '가격': last_price,
                                        'kg당가격': f"{last_price:,}원"
                                    })
                                    print(f"    [O] {grade}: {last_price:,}원/kg (최종판매가)")
                                part_success = True
                            else:
                                print(f"    [X] 최종 판매가도 찾을 수 없음")
                                failed_parts.append(part)
                            continue

                        # 정상적으로 상품 목록이 있는 경우
                        await page.wait_for_timeout(1000)

                        # 등급별 가격 수집 (등급 먼저 선택 → 정렬)
                        for j, grade in enumerate(grades, 1):
                            print(f"    [{grade} 등급 {j}/{len(grades)}] 수집 중...")
                            try:
                                # 1. 등급 필터 먼저 클릭
                                grade_clicked = await page.evaluate(f"""
                                    (grade) => {{
                                        const buttons = document.querySelectorAll('button');
                                        for (let btn of buttons) {{
                                            if (btn.textContent && btn.textContent.trim() === grade) {{
                                                btn.click();
                                                return true;
                                            }}
                                        }}
                                        return false;
                                    }}
                                """, grade)

                                if not grade_clicked:
                                    print(f"    [X] {grade} 버튼을 찾을 수 없음")
                                    continue

                                print(f"    {grade} 등급 필터 클릭")
                                await page.wait_for_timeout(2000)

                                # 등심 부위인 경우 추가 대기 (페이지 구조가 다를 수 있음)
                                if part == "등심":
                                    print(f"    등심 부위 - 추가 대기 중...")
                                    await page.wait_for_timeout(3000)

                                # 2. 등급 필터링 후 Kg당 낮은 가격순 정렬
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
                                    print(f"    Kg당 낮은 가격순 정렬")
                                    await page.wait_for_timeout(2000)

                                # 등심 부위인 경우 HTML 구조 디버깅
                                if part == "등심":
                                    debug_info = await page.evaluate("""
                                        () => {
                                            const products = document.querySelectorAll('.product-unit, .d-flex, .product-item, .pd-item');
                                            const prices = document.querySelectorAll('.pd-price, .price, [class*="price"]');

                                            return {
                                                productCount: products.length,
                                                priceCount: prices.length,
                                                firstProductHTML: products[0] ? products[0].outerHTML.substring(0, 500) : 'none',
                                                firstPriceText: prices[0] ? prices[0].textContent : 'none',
                                                firstPriceClass: prices[0] ? prices[0].className : 'none'
                                            };
                                        }
                                    """)
                                    print(f"    [디버그] 상품 수: {debug_info['productCount']}, 가격 요소 수: {debug_info['priceCount']}")
                                    print(f"    [디버그] 첫 가격 텍스트: {debug_info['firstPriceText']}")
                                    print(f"    [디버그] 첫 가격 클래스: {debug_info['firstPriceClass']}")

                                # 3. 최저가 수집 (테스트로 검증된 방식)
                                price = await page.evaluate("""
                                    () => {
                                        // 패턴 1: .pd-price.c-primary (테스트에서 성공)
                                        const priceEl1 = document.querySelector('.pd-price.c-primary');
                                        if (priceEl1) {
                                            const match = priceEl1.textContent.match(/([0-9,]+)/);
                                            if (match) {
                                                const price = parseInt(match[1].replace(/,/g, ''));
                                                if (price >= 10000 && price <= 1000000) {
                                                    console.log('Found price (pattern 1 - pd-price.c-primary):', price);
                                                    return price;
                                                }
                                            }
                                        }

                                        // 패턴 2: 모든 .pd-price 중 첫 번째
                                        const allPrices = document.querySelectorAll('.pd-price');
                                        for (let priceEl of allPrices) {
                                            const match = priceEl.textContent.match(/([0-9,]+)/);
                                            if (match) {
                                                const price = parseInt(match[1].replace(/,/g, ''));
                                                if (price >= 10000 && price <= 1000000) {
                                                    console.log('Found price (pattern 2 - all pd-price):', price);
                                                    return price;
                                                }
                                            }
                                        }

                                        // 패턴 3: .product-unit 안의 .pd-price.xs.c-primary
                                        let firstProduct = document.querySelector('.product-unit, .d-flex');
                                        if (firstProduct) {
                                            let priceEl = firstProduct.querySelector('.pd-price.xs.c-primary');
                                            if (priceEl) {
                                                const match = priceEl.textContent.match(/([0-9,]+)/);
                                                if (match) {
                                                    const price = parseInt(match[1].replace(/,/g, ''));
                                                    if (price >= 10000 && price <= 1000000) {
                                                        console.log('Found price (pattern 3 - in product-unit):', price);
                                                        return price;
                                                    }
                                                }
                                            }
                                        }

                                        console.log('No price found in any pattern');
                                        return null;
                                    }
                                """)

                                if price:
                                    self.market_wholesale_data.append({
                                        'date': datetime.now().strftime('%Y-%m-%d'),
                                        'source': '금천미트',
                                        'type': '부분육시장가',
                                        '축종': '한우',
                                        '부위': part,
                                        '등급': grade,
                                        '가격': price,
                                        'kg당가격': f"{price:,}원"
                                    })
                                    print(f"    [O] {grade}: {price:,}원/kg")
                                    part_success = True
                                else:
                                    print(f"    [X] {grade}: 가격 정보 없음")
                                    # 등심 부위에서 가격을 못 찾으면 스크린샷 저장
                                    if part == "등심":
                                        screenshot_name = f"debug_등심_{grade}.png"
                                        await page.screenshot(path=screenshot_name)
                                        print(f"    [디버그] 스크린샷 저장: {screenshot_name}")

                            except Exception as e:
                                print(f"    [X] {grade} 수집 오류: {e}")
                                continue

                        # 다음 부위를 위해 초기 페이지로
                        if i < len(parts_to_try):
                            await page.goto(beef_url, wait_until='domcontentloaded', timeout=30000)
                            await page.wait_for_selector('li.ctg-item', timeout=10000)

                    except Exception as e:
                        print(f"    [X] 오류: {e}")

                    if not part_success:
                        failed_parts.append(part)

                print(f"\n{attempt}차 완료: {len(parts_to_try) - len(failed_parts)}/{len(parts_to_try)}개 성공")

                if failed_parts and attempt < 3:
                    print(f"실패: {', '.join(failed_parts)}")

            # 뼈류 부위 수집 (1등급만)
            print(f"\n=== 한우 뼈류 가격 수집 (1등급만) ===")
            beef_url = "https://www.ekcm.co.kr/pd/product?dispCtgNo=14&dispCtgNm=%EA%B5%AD%EB%82%B4%EC%82%B0+%ED%95%9C%EC%9A%B0+%EC%95%94%EC%86%8C"

            for i, (internal_name, query_name) in enumerate(bone_parts.items(), 1):
                print(f"[뼈류 {i}/{len(bone_parts)}] {internal_name} ({query_name}) 수집 중...")
                try:
                    await page.goto(beef_url, wait_until='networkidle', timeout=60000)
                    await page.wait_for_timeout(3000)

                    # 부위 클릭
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
                                                matched: text
                                            }};
                                        }}
                                    }}
                                }}
                            }}
                            return {{ success: false }};
                        }}
                    """, query_name)

                    clicked = click_result.get('success', False)
                    if not clicked:
                        print(f"    [X] 부위를 찾을 수 없음")
                        continue

                    print(f"    [O] 클릭: '{click_result.get('matched', '')}'")
                    await page.wait_for_load_state('domcontentloaded', timeout=10000)
                    await page.wait_for_timeout(3000)

                    # 1등급만 선택
                    grade = "1"
                    grade_clicked = await page.evaluate(f"""
                        (grade) => {{
                            const buttons = document.querySelectorAll('button');
                            for (let btn of buttons) {{
                                if (btn.textContent && btn.textContent.trim() === grade) {{
                                    btn.click();
                                    return true;
                                }}
                            }}
                            return false;
                        }}
                    """, grade)

                    if grade_clicked:
                        print(f"    [1등급] 필터 클릭")
                        await page.wait_for_timeout(2000)

                    # 가격순 정렬
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
                        await page.wait_for_timeout(2000)

                    # 재고가격(현재가) 먼저 시도
                    price = await page.evaluate("""
                        () => {
                            const priceEl1 = document.querySelector('.pd-price.c-primary');
                            if (priceEl1) {
                                const match = priceEl1.textContent.match(/([0-9,]+)/);
                                if (match) {
                                    const price = parseInt(match[1].replace(/,/g, ''));
                                    if (price >= 500 && price <= 1000000) {
                                        return price;
                                    }
                                }
                            }

                            const allPrices = document.querySelectorAll('.pd-price');
                            for (let priceEl of allPrices) {
                                const match = priceEl.textContent.match(/([0-9,]+)/);
                                if (match) {
                                    const price = parseInt(match[1].replace(/,/g, ''));
                                    if (price >= 500 && price <= 1000000) {
                                        return price;
                                    }
                                }
                            }
                            return null;
                        }
                    """)

                    # 재고가격이 없으면 최종판매가 시도
                    if not price:
                        print(f"    재고가격 없음, 최종판매가 시도...")
                        price = await page.evaluate("""
                            () => {
                                const soldoutWrap = document.querySelector('.soldout-wrap');
                                if (soldoutWrap) {
                                    const priceRow = soldoutWrap.querySelector('dl.row.price');
                                    if (priceRow) {
                                        const priceEl = priceRow.querySelector('.pd-price.c-primary');
                                        if (priceEl) {
                                            const match = priceEl.textContent.match(/([0-9,]+)/);
                                            if (match) {
                                                const price = parseInt(match[1].replace(/,/g, ''));
                                                if (price >= 500 && price <= 1000000) {
                                                    return price;
                                                }
                                            }
                                        }
                                    }
                                }

                                const allText = document.body.innerText;
                                const lastPriceMatch = allText.match(/최종\\s*판매가[^0-9]*([0-9,]+)/i);
                                if (lastPriceMatch) {
                                    const price = parseInt(lastPriceMatch[1].replace(/,/g, ''));
                                    if (price >= 500 && price <= 1000000) {
                                        return price;
                                    }
                                }
                                return null;
                            }
                        """)

                    if price:
                        self.market_wholesale_data.append({
                            'date': datetime.now().strftime('%Y-%m-%d'),
                            'source': '금천미트',
                            'type': '부분육시장가',
                            '축종': '한우',
                            '부위': internal_name,  # 내부명칭 사용
                            '등급': grade,
                            '가격': price,
                            'kg당가격': f"{price:,}원"
                        })
                        print(f"    [O] {grade}: {price:,}원/kg")
                    else:
                        print(f"    [X] {grade}: 가격 정보 없음")

                except Exception as e:
                    print(f"    [X] {internal_name} 수집 오류: {e}")
                    continue

            return len(self.market_wholesale_data) > 0

        except Exception as e:
            self.log_error("한우수집", f"오류: {e}")
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
        """Excel 파일 저장 - 부분육과 도체경락가 통합 (2가지 형태)"""
        if filename is None:
            filename = f"beef_price_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        try:
            if not self.market_wholesale_data and not self.auction_data:
                print("저장할 데이터가 없습니다.")
                return False

            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # 모든 데이터 통합
                all_data = []

                # 부분육 시장가격 데이터 추가
                if self.market_wholesale_data:
                    all_data.extend(self.market_wholesale_data)

                # 도체 경락가격 데이터 추가
                if self.auction_data:
                    all_data.extend(self.auction_data)

                if all_data:
                    df = pd.DataFrame(all_data)

                    # type 정렬 순서: 도체경락가 → 부분육시장가
                    type_order = ['도체경락가', '부분육시장가']

                    # 부위 정렬 순서 (뼈류 포함)
                    part_order = [
                        '전체',  # 도체경락가
                        '안심', '등심', '채끝', '부채살',
                        '앞다리살', '업진살', '치마살', '제비추리',
                        '토시살', '안창살', '목심', '우둔살',
                        '설도', '양지머리외', '사태', '갈비', '차돌박이',
                        '사골', '꼬리', '잡뼈', '우족', '도가니', '스지'  # 뼈류
                    ]

                    # Categorical로 변환하여 순서 지정
                    df['type'] = pd.Categorical(df['type'], categories=type_order, ordered=True)
                    df['부위'] = pd.Categorical(df['부위'], categories=part_order, ordered=True)

                    # 정렬: type 먼저, 그 다음 부위, 마지막 등급
                    df_sorted = df.sort_values(['type', '부위', '등급'])

                    # 1. 원본 형태 (All_Data) - 모든 데이터를 행으로 나열
                    df_sorted.to_excel(writer, sheet_name='All_Data', index=False)

                    # 2. Pivot 형태 - 부위를 행으로, 등급을 열로
                    # 부분육 시장가만 pivot 테이블 생성
                    if self.market_wholesale_data:
                        market_df = df[df['type'] == '부분육시장가'].copy()
                        if not market_df.empty:
                            # Pivot 테이블: 부위 x 등급
                            pivot_df = market_df.pivot_table(
                                index='부위',
                                columns='등급',
                                values='가격',
                                aggfunc='first',
                                observed=False
                            )

                            # 부위 순서 정렬
                            parts_in_data = [p for p in part_order if p in pivot_df.index]
                            pivot_df = pivot_df.reindex(parts_in_data)

                            # 등급 순서 정렬
                            grade_order = ['1++', '1+', '1', '2']
                            existing_grades = [g for g in grade_order if g in pivot_df.columns]
                            pivot_df = pivot_df[existing_grades]

                            # Pivot 시트에 저장
                            pivot_df.to_excel(writer, sheet_name='Pivot_부위x등급')

            market_count = len(self.market_wholesale_data) if self.market_wholesale_data else 0
            auction_count = len(self.auction_data) if self.auction_data else 0

            print(f"\n=== Excel 파일 저장 완료 ===")
            print(f"파일명: {filename}")
            print(f"데이터: 부분육 {market_count}건, 도체경락가 {auction_count}건")
            print(f"시트: All_Data (원본), Pivot_부위x등급 (피벗)")
            return True

        except Exception as e:
            self.log_error("Excel저장", f"저장 실패: {e}")
            return False

    def print_summary(self):
        """수집 결과 요약 출력"""
        if not self.market_wholesale_data and not self.auction_data:
            print("수집된 데이터가 없습니다.")
            return

        print(f"\n=== 한우 데이터 수집 결과 요약 ===")

        # 도체 경락가 요약
        if self.auction_data:
            print(f"도체 경락가: {len(self.auction_data)}건")
            for data in self.auction_data:
                print(f"  - {data['등급']}등급: {data['가격']:,}원/kg")

        # 부분육 시장가 요약
        if self.market_wholesale_data:
            df = pd.DataFrame(self.market_wholesale_data)
            print(f"부분육 시장가: {len(df)}건")
            print(f"수집 부위: {df['부위'].nunique()}개")

            for part in sorted(df['부위'].unique()):
                part_df = df[df['부위'] == part]
                avg_price = part_df['가격'].mean()
                print(f"  - {part}: {avg_price:,.0f}원/kg")


# ============================================================
# MarginCalculatorCompare 클래스 (marginb_compare.py 원본)
# ============================================================

class MarginCalculatorCompare:
    def __init__(self, price_file, weight_file=None):
        self.price_file = price_file
        # 중량 파일은 항상 price0.xlsx 사용 (고정값)
        self.weight_file = weight_file or "price0.xlsx"
        self.grades = ["1++", "1+", "1", "2"]
        self.margins = [0.10, 0.20, 0.30, 0.40]

        # 뼈류 고정가격 (원/kg) - 완전 고정값
        self.bone_prices = {
            "사골": 1000,
            "꼬리": 4000,
            "잡뼈": 1000,
            "우족": 2000,
            "도가니": 6000,
            "스지": 3000
        }

        # 뼈류 등급별 중량 데이터 (book1.xlsx 기준) - [부위명, 1++, 1+, 1, 2등급 순서]
        self.bone_parts_weights = [
            ['우족', 7.44, 5.71, 5.90, 5.96],
            ['꼬리', 11.79, 9.48, 9.79, 10.03],
            ['사골', 17.28, 14.11, 15.14, 15.16],
            ['도가니', 2.29, 1.70, 1.73, 1.62],
            ['잡뼈', 20.23, 16.54, 17.02, 17.43],
            ['스지', 3.0, 2.5, 2.6, 2.7]
        ]

        # 뼈류 고정가격 (원/kg)
        self.bone_part_prices = {
            "사골": 1000,
            "꼬리": 4000,
            "잡뼈": 1000,
            "우족": 2000,
            "도가니": 6000,
            "스지": 3000
        }

    def load_data(self):
        """데이터 로드"""
        print("데이터 로딩 중...")

        # 가격 데이터 로드 (경락가 + 도매가)
        try:
            self.df_price = pd.read_excel(self.price_file, sheet_name='All_Data')
            print(f"가격 데이터 로드 완료: {len(self.df_price)}건")
        except Exception as e:
            print(f"가격 데이터 로드 실패: {e}")
            return False

        return True

    def prepare_data(self):
        """데이터 전처리"""
        # 경락가 데이터 분리
        self.auction_data = self.df_price[self.df_price['type'] == '도체경락가'].copy()
        if not self.auction_data.empty:
            self.auction_data = self.auction_data.rename(columns={'Grade': '등급', 'Price': '가격'})

        # 부분육 도매가 데이터 분리
        self.market_data = self.df_price[self.df_price['type'] == '부분육시장가'].copy()
        if not self.market_data.empty:
            self.market_data = self.market_data.rename(columns={'Part': '부위', 'Grade': '등급', 'Price': '가격'})

        # 도매가 pivot 테이블 생성
        if not self.market_data.empty:
            self.market_pivot = self.market_data.pivot_table(index="부위", columns="등급", values="가격", aggfunc="last")
        else:
            self.market_pivot = pd.DataFrame()

        # 중량 데이터 처리
        grade_cols = {"1++":"1++등급","1+":"1+등급","1":"1등급.1","2":"2등급"}

        # 냉도체중 (book1.xlsx 고정값)
        self.carcass_weights = {
            "1++": 477.23,
            "1+": 385.53,
            "1": 393.12,
            "2": 377.22
        }

        # 부대비용 (고정값)
        self.overhead_default = 770000.0

        # 22개 부위별 중량 데이터 (book1.xlsx 고정값) - [부위명, 1++, 1+, 1, 2등급 순서]
        fixed_parts_weights = [
            ['안심', 7.6, 6.7, 6.8, 6.7],
            ['등심', 41.8, 33.7, 34.4, 32.4],
            ['채끝', 12.0, 9.5, 9.4, 8.8],
            ['목심', 20.1, 16.3, 16.8, 16.1],
            ['앞다리', 15.9, 13.8, 14.3, 13.8],
            ['우둔', 23.5, 21.2, 21.8, 21.8],
            ['설도', 47.6, 40.6, 40.3, 39.4],
            ['사태', 19.8, 17.8, 18.7, 19.6],
            ['양지', 35.6, 28.1, 29.4, 28.5],
            ['갈비', 58.0, 46.7, 47.8, 46.0],
            ['차돌박이', 7.6, 6.1, 6.6, 6.3],
            ['부채살', 4.9, 4.2, 4.3, 4.2],
            ['업진살', 2.1, 1.7, 1.7, 1.7],  # book1.xlsx의 업진살 (업진안살 가격 사용)
            ['치마살', 4.1, 3.4, 3.4, 3.3],
            ['제비추리', 0.9, 0.8, 0.7, 0.8],
            ['토시살', 1.1, 0.9, 0.9, 0.9],
            ['안창살', 1.8, 1.6, 1.6, 1.5]
        ]

        # DataFrame 생성
        parts_data = []
        for part_data in fixed_parts_weights:
            part_row = {
                '부위': part_data[0],
                '1++': part_data[1],
                '1+': part_data[2],
                '1': part_data[3],
                '2': part_data[4]
            }
            parts_data.append(part_row)

        self.cut_weights = pd.DataFrame(parts_data)

        print("데이터 전처리 완료")
        return True

    def get_market_price(self, part, grade, use_markup=False):
        """부위별 도매가격 조회

        use_markup: True면 10% 할증, False면 원가 그대로
        """
        # 뼈류인 경우: 1등급 기준으로 조회
        if part in self.bone_part_prices:
            # 시장가격 먼저 확인 (1등급만)
            if not self.market_pivot.empty and part in self.market_pivot.index:
                if "1" in self.market_pivot.columns:
                    val = self.market_pivot.loc[part, "1"]
                    if pd.notna(val):
                        base_price = float(val)
                        if use_markup:
                            return base_price * 1.10
                        else:
                            return base_price
            # 시장가격이 없으면 고정가 사용
            return float(self.bone_part_prices[part])

        # 일반 부위: 부위명 매핑 (계산기 부위명 -> 가격데이터 부위명)
        part_mapping = {
            '우둔': '우둔살',
            '양지': '양지머리외',
            '앞다리': '앞다리살',
            '업진살': '업진살'
        }

        # 매핑된 부위명 사용
        market_part = part_mapping.get(part, part)

        # 시장가격 조회
        if not self.market_pivot.empty and market_part in self.market_pivot.index:
            # 해당 등급 가격 확인
            if grade in self.market_pivot.columns:
                val = self.market_pivot.loc[market_part, grade]
                if pd.notna(val):
                    base_price = float(val)
                    if use_markup:
                        return base_price * 1.10
                    else:
                        return base_price

            # 폴백: 다른 등급 가격
            for fallback_grade in ["1", "1+", "1++", "2"]:
                if fallback_grade in self.market_pivot.columns:
                    val = self.market_pivot.loc[market_part, fallback_grade]
                    if pd.notna(val):
                        base_price = float(val)
                        if use_markup:
                            return base_price * 1.10
                        else:
                            return base_price

        return np.nan

    def compute_compare_table(self, grade):
        """등급별 적수원가/마진 비교 계산 (기존 방식 + 새로운 방식)"""
        # 경락가 확인
        auction_rows = self.auction_data[self.auction_data["등급"] == grade]
        if auction_rows.empty:
            print(f"{grade} 등급 경락가 데이터 없음")
            return pd.DataFrame()

        auction_price = float(auction_rows["가격"].iloc[0])
        carcass_weight = self.carcass_weights[grade]
        total_cost = carcass_weight * auction_price + self.overhead_default

        # 부위별 중량 데이터 준비
        parts_data = []

        # 일반 부위 (price0.xlsx에서)
        if grade in self.cut_weights.columns:
            for _, row in self.cut_weights.iterrows():
                try:
                    weight = float(row[grade])
                    if weight > 0:
                        parts_data.append({
                            "부위": row["부위"],
                            "중량(kg)": weight
                        })
                except:
                    continue

        # 뼈류 추가 (등급별 중량 적용, 가격은 1등급 기준)
        for bone_data in self.bone_parts_weights:
            bone_name = bone_data[0]
            # 등급별 중량 선택 (1++:1, 1+:2, 1:3, 2:4 인덱스)
            grade_index = {"1++": 1, "1+": 2, "1": 3, "2": 4}[grade]
            bone_weight = bone_data[grade_index]

            # 뼈류는 항상 1등급 기준으로 가격 조회 (경락가 기반)
            bone_price = None
            if not self.market_pivot.empty and bone_name in self.market_pivot.index:
                if "1" in self.market_pivot.columns:
                    val = self.market_pivot.loc[bone_name, "1"]
                    if pd.notna(val):
                        bone_price = float(val)

            # 시장가격이 없으면 고정가 사용
            if bone_price is None:
                bone_price = self.bone_part_prices[bone_name]

            bone_value = bone_weight * bone_price

            parts_data.append({
                "부위": bone_name,
                "중량(kg)": bone_weight,
                "시장가격(원/kg)": bone_price,
                "시장가치(원)": bone_value
            })

        if not parts_data:
            return pd.DataFrame()

        df = pd.DataFrame(parts_data)

        # 시장가격이 없는 부위만 조회 (뼈류는 이미 설정됨) - 경락가 기반 (원가 그대로)
        bone_part_names = [bone[0] for bone in self.bone_parts_weights]
        mask_no_price = ~df["부위"].isin(bone_part_names)
        if mask_no_price.any():
            df.loc[mask_no_price, "시장가격(원/kg)"] = df.loc[mask_no_price, "부위"].apply(lambda x: self.get_market_price(x, grade, use_markup=False))

        df = df.dropna(subset=["시장가격(원/kg)"]).reset_index(drop=True)

        if df.empty:
            return pd.DataFrame()

        # 시장가치가 없는 부위만 계산 (뼈류는 이미 설정됨)
        mask_no_value = ~df["부위"].isin(bone_part_names)
        if mask_no_value.any():
            df.loc[mask_no_value, "시장가치(원)"] = df.loc[mask_no_value, "중량(kg)"] * df.loc[mask_no_value, "시장가격(원/kg)"]

        virtual_total = df["시장가치(원)"].sum()

        # === 경락가 기반 방식 (원가 그대로) ===
        df["적수비"] = df["시장가치(원)"] / virtual_total if virtual_total > 0 else 0.0
        df["적수합계(원)"] = total_cost * df["적수비"]
        df["경락가_적수원가(원/kg)"] = df["적수합계(원)"] / df["중량(kg)"]

        # 현재 마진율 계산 (경락가 기반)
        df["경락가_현재마진율(%)"] = np.round(((df["시장가격(원/kg)"] - df["경락가_적수원가(원/kg)"]) / df["경락가_적수원가(원/kg)"]) * 100, 1)

        # 경락가 기반 마진별 판매가
        for margin in self.margins:
            df[f"경락가_{int(margin*100)}%마진"] = np.round(df["경락가_적수원가(원/kg)"] * (1 + margin), 0).astype(int)

        # === 금천미트 10% 마진 방식 (신규) ===
        # 시장가격을 10% 마진이 포함된 가격으로 간주하고 원가 역산
        df["금천10%_시장가격(원/kg)"] = df["시장가격(원/kg)"]  # 경락가 기반과 동일한 시장가격 사용
        df["금천10%_적수원가(원/kg)"] = df["금천10%_시장가격(원/kg)"] / 1.10  # 10% 마진 역산
        df["금천10%_적수합계(원)"] = df["금천10%_적수원가(원/kg)"] * df["중량(kg)"]

        # 적수비 계산
        total_cost_calculated = df["금천10%_적수합계(원)"].sum()
        df["금천10%_적수비"] = df["금천10%_적수합계(원)"] / total_cost_calculated if total_cost_calculated > 0 else 0.0
        df["금천10%_시장가치(원)"] = df["중량(kg)"] * df["금천10%_시장가격(원/kg)"]

        # 금천10% 현재 마진율은 고정 10% (가정)
        df["금천10%_현재마진율(%)"] = 10.0

        # 금천10% 마진별 가격 계산
        for margin in self.margins:
            margin_price = df["금천10%_적수원가(원/kg)"] * (1 + margin)
            df[f"금천10%_{int(margin*100)}%마진"] = np.round(margin_price, 0).astype(int)

        # 적수원가 차이 계산
        df["적수원가_차이(원/kg)"] = df["경락가_적수원가(원/kg)"] - df["금천10%_적수원가(원/kg)"]
        df["적수원가_차이율(%)"] = np.round(((df["경락가_적수원가(원/kg)"] - df["금천10%_적수원가(원/kg)"]) / df["금천10%_적수원가(원/kg)"]) * 100, 1)

        # 메타정보 추가
        df["등급"] = grade
        df["경락가(원/kg)"] = auction_price
        df["냉도체중(kg)"] = carcass_weight
        df["부대비용(원)"] = self.overhead_default
        df["총원가(원)"] = int(round(total_cost))

        # 컬럼 순서 정리
        cols = (["등급", "부위", "중량(kg)",
                "시장가격(원/kg)", "경락가_적수원가(원/kg)", "경락가_현재마진율(%)",
                "경락가_10%마진", "경락가_20%마진", "경락가_30%마진", "경락가_40%마진",
                "금천10%_시장가격(원/kg)", "금천10%_적수원가(원/kg)", "금천10%_현재마진율(%)",
                "금천10%_10%마진", "금천10%_20%마진", "금천10%_30%마진", "금천10%_40%마진",
                "적수원가_차이(원/kg)", "적수원가_차이율(%)",
                "시장가치(원)", "적수비", "적수합계(원)",
                "금천10%_시장가치(원)", "금천10%_적수비", "금천10%_적수합계(원)",
                "경락가(원/kg)", "냉도체중(kg)", "부대비용(원)", "총원가(원)"])

        return df[cols]

    def generate_results(self):
        """모든 등급에 대해 계산 실행"""
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

    def _generate_html_consolidated_table(self):
        """금천미트 10% 마진가격표 통합표 HTML 생성"""
        grade_colors = {"1++": "#E74C3C", "1+": "#E67E22", "1": "#2980B9", "2": "#7F8C8D"}

        # 모든 등급에서 공통 부위 목록 생성
        all_parts = set()
        for grade_df in self.results.values():
            all_parts.update(grade_df['부위'].tolist())

        # 부위 순서 정렬 (기존 순서 유지)
        if self.results:
            first_grade_df = next(iter(self.results.values()))
            ordered_parts = first_grade_df['부위'].tolist()
            # 다른 등급에만 있는 부위들 추가
            for part in sorted(all_parts):
                if part not in ordered_parts:
                    ordered_parts.append(part)
        else:
            ordered_parts = sorted(all_parts)

        # 헤더
        header = f"""
        <div class='card'>
            <div class='card-header' style='background:#a8e6cf;color:#2c3e50;padding:14px 20px;'>
                <span style='font-size:18px;font-weight:700'>금천미트 10% 할증 마진가격표 (전등급 통합)</span>
            </div>
        """

        # 테이블 헤더
        table_html = "<div style='overflow:auto'><table style='width:100%;border-collapse:collapse;font-size:12px'>"
        table_html += """
        <thead>
            <tr style='background:#fbfcfe'>
                <th rowspan="2" style='padding:8px;border:1px solid #eee;text-align:center;vertical-align:middle'>부위</th>
        """

        # 등급별 헤더 (4개 마진 컬럼 병합)
        for grade in self.grades:
            if grade in self.results:
                table_html += f"""
                <th colspan="4" style='padding:8px;border:1px solid #eee;text-align:center;background:{grade_colors[grade]};color:white;font-weight:bold'>{grade}급</th>
                """

        table_html += """
            </tr>
            <tr style='background:#fbfcfe'>
        """

        # 마진율 헤더 (각 등급마다 반복)
        for grade in self.grades:
            if grade in self.results:
                for margin in [10, 20, 30, 40]:
                    table_html += f"""
                    <th style='padding:6px;border:1px solid #eee;text-align:center'>{margin}%마진</th>
                    """

        table_html += """
            </tr>
        </thead><tbody>
        """

        # 데이터 행
        for part in ordered_parts:
            table_html += f"""
            <tr>
                <td style='padding:6px;border:1px solid #eee;font-weight:500'>{part}</td>
            """

            # 각 등급별 마진 가격
            for grade in self.grades:
                if grade in self.results:
                    grade_df = self.results[grade]
                    part_rows = grade_df[grade_df['부위'] == part]

                    if not part_rows.empty:
                        row = part_rows.iloc[0]
                        # 금천10% 마진 가격 4개
                        for margin_pct in [10, 20, 30, 40]:
                            col_name = f'금천10%_{margin_pct}%마진'
                            price = int(row[col_name])
                            table_html += f"""
                            <td style='padding:6px;border:1px solid #eee;text-align:right'>{price:,}</td>
                            """
                    else:
                        # 해당 부위가 이 등급에 없음
                        for _ in range(4):
                            table_html += """
                            <td style='padding:6px;border:1px solid #eee;text-align:center;color:#ccc'>-</td>
                            """

            table_html += """
            </tr>
            """

        table_html += "</tbody></table></div></div>"

        return header + table_html

    def export_html(self, filename=None):
        """HTML 결과 생성"""
        if filename is None:
            filename = f"beef_margin_compare_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"

        # HTML 생성
        sections = []
        grade_colors = {"1++": "#E74C3C", "1+": "#E67E22", "1": "#2980B9", "2": "#7F8C8D"}
        grade_titles = {"1++": "1++ 등급 (최고급)", "1+": "1+ 등급 (특급)", "1": "1 등급 (우수)", "2": "2 등급 (일반)"}

        for grade, df in self.results.items():
            if df.empty:
                continue

            virtual_total = df["시장가치(원)"].sum()
            total_cost = df["총원가(원)"].iloc[0]
            total_weight = df["중량(kg)"].sum()

            # 헤더
            header = f"""
            <div class='card'>
                <div class='card-header' style='background:{grade_colors[grade]};color:white;padding:14px 20px;'>
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

            # 테이블
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

        # 금천미트 10% 마진가격표 통합표 추가
        consolidated_section = self._generate_html_consolidated_table()
        sections.append(consolidated_section)

        # 전체 HTML 구성
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
            <title>한우 적수원가 계산 방식 비교</title>
            <style>{css}</style>
        </head>
        <body>
            <h1>한우 적수원가 계산 방식 비교</h1>
            <div style='background:#e8f4f8;padding:15px;border-radius:8px;margin:20px 0'>
                <h3 style='margin-top:0;color:#2c3e50'>📊 계산 방식 설명</h3>
                <div style='display:flex;gap:20px'>
                    <div style='flex:1;background:#ffeaa7;padding:10px;border-radius:5px'>
                        <strong>🔸 경락가 기반 (원가 그대로)</strong><br/>
                        금천미트 시장가격 원가 그대로 사용<br/>
                        경매가 기준 적수비 적용 → 적수원가 계산
                    </div>
                    <div style='flex:1;background:#a8e6cf;padding:10px;border-radius:5px'>
                        <strong>🔸 금천미트 10% 할증</strong><br/>
                        금천미트 시장가격 × 1.10<br/>
                        할증 가격 기준 적수비 적용 → 적수원가 계산
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
        """Excel 결과 생성"""
        if filename is None:
            filename = f"beef_margin_compare_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        wb = xlsxwriter.Workbook(filename)

        # 포맷 정의
        fmt_header = wb.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#fbfcfe'})
        fmt_text = wb.add_format({'border': 1})
        fmt_number = wb.add_format({'border': 1, 'num_format': '#,##0'})
        fmt_decimal = wb.add_format({'border': 1, 'num_format': '#,##0.00'})
        fmt_percent = wb.add_format({'border': 1, 'num_format': '0.0'})

        # 방식별 색상
        fmt_original = wb.add_format({'border': 1, 'num_format': '#,##0', 'bg_color': '#ffeaa7'})
        fmt_new = wb.add_format({'border': 1, 'num_format': '#,##0', 'bg_color': '#a8e6cf'})
        fmt_diff_pos = wb.add_format({'border': 1, 'num_format': '#,##0', 'bg_color': '#ffe6e6', 'bold': True})
        fmt_diff_neg = wb.add_format({'border': 1, 'num_format': '#,##0', 'bg_color': '#e6ffe6', 'bold': True})

        grade_colors = {"1++": "#E74C3C", "1+": "#E67E22", "1": "#2980B9", "2": "#7F8C8D"}
        grade_titles = {"1++": "1++ 등급 (최고급)", "1+": "1+ 등급 (특급)", "1": "1 등급 (우수)", "2": "2 등급 (일반)"}

        for grade, df in self.results.items():
            if df.empty:
                continue

            ws = wb.add_worksheet(grade)

            # 등급 헤더
            header_format = wb.add_format({
                'bold': True, 'font_color': 'white', 'bg_color': grade_colors[grade],
                'align': 'left', 'font_size': 12, 'border': 1
            })
            ws.merge_range(0, 0, 0, 17, f"{grade_titles[grade]} - 적수원가 계산 방식 비교", header_format)

            # 메타 정보
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

            # 테이블 헤더 (2행 구조)
            # 1행: 그룹 헤더
            ws.merge_range(4, 0, 5, 0, "부위", fmt_header)
            ws.merge_range(4, 1, 5, 1, "중량(kg)", fmt_header)
            ws.merge_range(4, 2, 5, 2, "시장가격(원/kg)", fmt_header)

            # 경락가 기반 헤더
            fmt_auction_header = wb.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#ffeaa7'})
            ws.merge_range(4, 3, 4, 8, "경락가 기반 (원가 그대로)", fmt_auction_header)

            # 금천10% 헤더
            fmt_markup_header = wb.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#a8e6cf'})
            ws.merge_range(4, 9, 4, 13, "금천미트 10% 할증", fmt_markup_header)

            # 차이 분석 헤더
            fmt_diff_header = wb.add_format({'bold': True, 'align': 'center', 'border': 1, 'bg_color': '#ffb3ba'})
            ws.merge_range(4, 14, 4, 15, "차이 분석", fmt_diff_header)

            # 2행: 상세 헤더
            headers = ["적수원가", "현재마진율", "10%마진", "20%마진", "30%마진", "40%마진",
                      "적수원가", "10%마진", "20%마진", "30%마진", "40%마진",
                      "원가차이", "차이율(%)"]

            for j, header in enumerate(headers, start=3):
                ws.write(5, j, header, fmt_header)

            # 데이터
            last_row = 6
            for i, (_, row) in enumerate(df.iterrows(), start=6):
                ws.write(i, 0, row['부위'], fmt_text)
                ws.write(i, 1, float(row['중량(kg)']), fmt_decimal)
                ws.write(i, 2, float(row['시장가격(원/kg)']), fmt_number)

                # 경락가 기반
                ws.write(i, 3, float(row['경락가_적수원가(원/kg)']), fmt_original)
                ws.write(i, 4, float(row['경락가_현재마진율(%)']), fmt_percent)
                ws.write(i, 5, int(row['경락가_10%마진']), fmt_original)
                ws.write(i, 6, int(row['경락가_20%마진']), fmt_original)
                ws.write(i, 7, int(row['경락가_30%마진']), fmt_original)
                ws.write(i, 8, int(row['경락가_40%마진']), fmt_original)

                # 금천10%
                ws.write(i, 9, float(row['금천10%_적수원가(원/kg)']), fmt_new)
                ws.write(i, 10, int(row['금천10%_10%마진']), fmt_new)
                ws.write(i, 11, int(row['금천10%_20%마진']), fmt_new)
                ws.write(i, 12, int(row['금천10%_30%마진']), fmt_new)
                ws.write(i, 13, int(row['금천10%_40%마진']), fmt_new)

                # 차이 분석
                diff_fmt = fmt_diff_pos if row['적수원가_차이(원/kg)'] > 0 else fmt_diff_neg
                ws.write(i, 14, float(row['적수원가_차이(원/kg)']), diff_fmt)
                ws.write(i, 15, float(row['적수원가_차이율(%)']), diff_fmt)

                last_row = i

            # 컬럼 너비 조정
            ws.set_column(0, 0, 12)   # 부위
            ws.set_column(1, 1, 10)   # 중량
            ws.set_column(2, 15, 11)  # 나머지 컬럼들

            # 틀 고정
            ws.freeze_panes(6, 1)

        # 통합표 시트 추가 (경락가기반/금천10% 분리)
        self._create_auction_consolidated_sheet(wb)
        self._create_markup_consolidated_sheet(wb)
        # 금천미트 10% 마진가격만 표시하는 간결한 통합표 추가
        self._create_margin_price_only_sheet(wb)

        wb.close()
        print(f"Excel 결과 저장: {filename}")
        return filename

    def _create_auction_consolidated_sheet(self, wb):
        """경락가 기반 통합표 시트 생성"""
        ws = wb.add_worksheet("경락가기반_통합표")

        # 가로형태로 출력 고려 - 페이지 설정
        ws.set_landscape()
        ws.set_paper(9)  # A4
        ws.set_margins(0.5, 0.5, 0.7, 0.7)

        # 통일된 포맷 정의
        base_format = {
            'font_name': 'Malgun Gothic',
            'font_size': 10,
            'border': 1,
            'border_color': '#CCCCCC'
        }

        fmt_title = wb.add_format({
            **base_format,
            'bold': True, 'align': 'center',
            'bg_color': '#ffeaa7', 'font_color': 'black', 'font_size': 12
        })
        fmt_col_header = wb.add_format({
            **base_format,
            'bold': True, 'align': 'center',
            'bg_color': '#fbfcfe'
        })
        fmt_part_name = wb.add_format({
            **base_format,
            'align': 'left'
        })
        fmt_number = wb.add_format({
            **base_format,
            'align': 'right',
            'num_format': '#,##0'
        })

        # 등급별 색상
        grade_colors = {"1++": "#E74C3C", "1+": "#E67E22", "1": "#2980B9", "2": "#7F8C8D"}

        # 제목 행
        ws.merge_range(0, 0, 0, 24, "경락가 기반 통합표 (원가 그대로)", fmt_title)

        # 헤더 생성
        # Row 1: 등급 헤더 (병합된 셀)
        col_start = 1  # A열은 부위명용
        for grade in self.grades:
            if grade in self.results:
                # 각 등급마다 6개 컬럼 (적수원가 + 현재마진율 + 4개 마진)
                grade_format = wb.add_format({
                    **base_format,
                    'bold': True, 'align': 'center',
                    'bg_color': grade_colors[grade], 'font_color': 'white', 'font_size': 11
                })
                ws.merge_range(1, col_start, 1, col_start + 5, f"{grade}급", grade_format)
                col_start += 6

        # Row 2: 컬럼 헤더
        ws.write(2, 0, "부위", fmt_col_header)
        col = 1
        for grade in self.grades:
            if grade in self.results:
                ws.write(2, col, "적수원가", fmt_col_header)
                ws.write(2, col + 1, "현재마진율", fmt_col_header)
                ws.write(2, col + 2, "10%마진", fmt_col_header)
                ws.write(2, col + 3, "20%마진", fmt_col_header)
                ws.write(2, col + 4, "30%마진", fmt_col_header)
                ws.write(2, col + 5, "40%마진", fmt_col_header)
                col += 6

        # 모든 등급에서 공통 부위 목록 생성
        all_parts = set()
        for grade_df in self.results.values():
            all_parts.update(grade_df['부위'].tolist())

        # 부위 순서 정렬 (기존 순서 유지)
        if self.results:
            first_grade_df = next(iter(self.results.values()))
            ordered_parts = first_grade_df['부위'].tolist()
            # 다른 등급에만 있는 부위들 추가
            for part in sorted(all_parts):
                if part not in ordered_parts:
                    ordered_parts.append(part)
        else:
            ordered_parts = sorted(all_parts)

        # 데이터 행 생성 (수식 사용)
        for row_idx, part in enumerate(ordered_parts, start=3):
            ws.write(row_idx, 0, part, fmt_part_name)

            col = 1
            for grade in self.grades:
                if grade in self.results:
                    grade_df = self.results[grade]
                    part_rows = grade_df[grade_df['부위'] == part]

                    if not part_rows.empty:
                        # 해당 부위가 이 등급에 존재함 - 수식으로 참조
                        part_row_in_sheet = None
                        # 개별 등급 시트에서 이 부위의 행 번호 찾기
                        for sheet_row_idx, (_, sheet_row) in enumerate(grade_df.iterrows(), start=6):
                            if sheet_row['부위'] == part:
                                part_row_in_sheet = sheet_row_idx + 1  # Excel 행 번호 (1부터 시작)
                                break

                        if part_row_in_sheet:
                            # 수식으로 참조 (D열=기존적수원가, E열=현재마진율, F~I열=기존마진들)
                            ws.write_formula(row_idx, col, f"=ROUND('{grade}'!D{part_row_in_sheet},0)", fmt_number)
                            ws.write_formula(row_idx, col + 1, f"=ROUND('{grade}'!E{part_row_in_sheet},1)", fmt_number)
                            ws.write_formula(row_idx, col + 2, f"=ROUND('{grade}'!F{part_row_in_sheet},0)", fmt_number)
                            ws.write_formula(row_idx, col + 3, f"=ROUND('{grade}'!G{part_row_in_sheet},0)", fmt_number)
                            ws.write_formula(row_idx, col + 4, f"=ROUND('{grade}'!H{part_row_in_sheet},0)", fmt_number)
                            ws.write_formula(row_idx, col + 5, f"=ROUND('{grade}'!I{part_row_in_sheet},0)", fmt_number)
                    else:
                        # 해당 부위가 이 등급에 없음 - 빈 셀
                        empty_format = wb.add_format(base_format)
                        for i in range(6):
                            ws.write(row_idx, col + i, "", empty_format)

                    col += 6

        # 컬럼 너비 조정 (가로형태 고려)
        ws.set_column(0, 0, 10)  # 부위명
        for i in range(1, col):
            ws.set_column(i, i, 8)  # 가격 컬럼들 (좁게 조정)

        # 틀 고정
        ws.freeze_panes(3, 1)

        # 인쇄 설정
        ws.fit_to_pages(1, 0)  # 가로 1페이지에 맞춤

    def _create_markup_consolidated_sheet(self, wb):
        """금천미트 10% 할증 통합표 시트 생성"""
        ws = wb.add_worksheet("금천10%_통합표")

        # 가로형태로 출력 고려 - 페이지 설정
        ws.set_landscape()
        ws.set_paper(9)  # A4
        ws.set_margins(0.5, 0.5, 0.7, 0.7)

        # 통일된 포맷 정의
        base_format = {
            'font_name': 'Malgun Gothic',
            'font_size': 10,
            'border': 1,
            'border_color': '#CCCCCC'
        }

        fmt_title = wb.add_format({
            **base_format,
            'bold': True, 'align': 'center',
            'bg_color': '#a8e6cf', 'font_color': 'black', 'font_size': 12
        })
        fmt_col_header = wb.add_format({
            **base_format,
            'bold': True, 'align': 'center',
            'bg_color': '#fbfcfe'
        })
        fmt_part_name = wb.add_format({
            **base_format,
            'align': 'left'
        })
        fmt_number = wb.add_format({
            **base_format,
            'align': 'right',
            'num_format': '#,##0'
        })

        # 등급별 색상
        grade_colors = {"1++": "#E74C3C", "1+": "#E67E22", "1": "#2980B9", "2": "#7F8C8D"}

        # 제목 행
        ws.merge_range(0, 0, 0, 20, "금천미트 10% 할증 통합표", fmt_title)

        # 헤더 생성
        # Row 1: 등급 헤더 (병합된 셀)
        col_start = 1  # A열은 부위명용
        for grade in self.grades:
            if grade in self.results:
                # 각 등급마다 5개 컬럼 (적수원가 + 4개 마진)
                grade_format = wb.add_format({
                    **base_format,
                    'bold': True, 'align': 'center',
                    'bg_color': grade_colors[grade], 'font_color': 'white', 'font_size': 11
                })
                ws.merge_range(1, col_start, 1, col_start + 4, f"{grade}급", grade_format)
                col_start += 5

        # Row 2: 컬럼 헤더
        ws.write(2, 0, "부위", fmt_col_header)
        col = 1
        for grade in self.grades:
            if grade in self.results:
                ws.write(2, col, "적수원가", fmt_col_header)
                ws.write(2, col + 1, "10%마진", fmt_col_header)
                ws.write(2, col + 2, "20%마진", fmt_col_header)
                ws.write(2, col + 3, "30%마진", fmt_col_header)
                ws.write(2, col + 4, "40%마진", fmt_col_header)
                col += 5

        # 모든 등급에서 공통 부위 목록 생성
        all_parts = set()
        for grade_df in self.results.values():
            all_parts.update(grade_df['부위'].tolist())

        # 부위 순서 정렬 (기존 순서 유지)
        if self.results:
            first_grade_df = next(iter(self.results.values()))
            ordered_parts = first_grade_df['부위'].tolist()
            # 다른 등급에만 있는 부위들 추가
            for part in sorted(all_parts):
                if part not in ordered_parts:
                    ordered_parts.append(part)
        else:
            ordered_parts = sorted(all_parts)

        # 데이터 행 생성 (수식 사용)
        for row_idx, part in enumerate(ordered_parts, start=3):
            ws.write(row_idx, 0, part, fmt_part_name)

            col = 1
            for grade in self.grades:
                if grade in self.results:
                    grade_df = self.results[grade]
                    part_rows = grade_df[grade_df['부위'] == part]

                    if not part_rows.empty:
                        # 해당 부위가 이 등급에 존재함 - 수식으로 참조
                        part_row_in_sheet = None
                        # 개별 등급 시트에서 이 부위의 행 번호 찾기
                        for sheet_row_idx, (_, sheet_row) in enumerate(grade_df.iterrows(), start=6):
                            if sheet_row['부위'] == part:
                                part_row_in_sheet = sheet_row_idx + 1  # Excel 행 번호 (1부터 시작)
                                break

                        if part_row_in_sheet:
                            # 수식으로 참조 (J열=신규적수원가, K~N열=신규마진들)
                            ws.write_formula(row_idx, col, f"=ROUND('{grade}'!J{part_row_in_sheet},0)", fmt_number)
                            ws.write_formula(row_idx, col + 1, f"=ROUND('{grade}'!K{part_row_in_sheet},0)", fmt_number)
                            ws.write_formula(row_idx, col + 2, f"=ROUND('{grade}'!L{part_row_in_sheet},0)", fmt_number)
                            ws.write_formula(row_idx, col + 3, f"=ROUND('{grade}'!M{part_row_in_sheet},0)", fmt_number)
                            ws.write_formula(row_idx, col + 4, f"=ROUND('{grade}'!N{part_row_in_sheet},0)", fmt_number)
                    else:
                        # 해당 부위가 이 등급에 없음 - 빈 셀
                        empty_format = wb.add_format(base_format)
                        for i in range(5):
                            ws.write(row_idx, col + i, "", empty_format)

                    col += 5

        # 컬럼 너비 조정 (가로형태 고려)
        ws.set_column(0, 0, 10)  # 부위명
        for i in range(1, col):
            ws.set_column(i, i, 9)  # 가격 컬럼들 (좁게 조정)

        # 틀 고정
        ws.freeze_panes(3, 1)

        # 인쇄 설정
        ws.fit_to_pages(1, 0)  # 가로 1페이지에 맞춤

    def _create_margin_price_only_sheet(self, wb):
        """금천미트 10% 마진가격만 표시하는 간결한 통합표 (17열)"""
        ws = wb.add_worksheet("금천10%_마진가격표")

        # 가로형태로 출력 고려 - 페이지 설정
        ws.set_landscape()
        ws.set_paper(9)  # A4
        ws.set_margins(0.5, 0.5, 0.7, 0.7)

        # 통일된 포맷 정의
        base_format = {
            'font_name': 'Malgun Gothic',
            'font_size': 10,
            'border': 1,
            'border_color': '#CCCCCC'
        }

        fmt_title = wb.add_format({
            **base_format,
            'bold': True, 'align': 'center',
            'bg_color': '#a8e6cf', 'font_color': 'black', 'font_size': 12
        })
        fmt_col_header = wb.add_format({
            **base_format,
            'bold': True, 'align': 'center',
            'bg_color': '#fbfcfe'
        })
        fmt_part_name = wb.add_format({
            **base_format,
            'align': 'left'
        })
        fmt_number = wb.add_format({
            **base_format,
            'align': 'right',
            'num_format': '#,##0'
        })

        # 등급별 색상
        grade_colors = {"1++": "#E74C3C", "1+": "#E67E22", "1": "#2980B9", "2": "#7F8C8D"}

        # 제목 행
        ws.merge_range(0, 0, 0, 16, "금천미트 10% 할증 마진가격표", fmt_title)

        # 헤더 생성
        # Row 1: 등급 헤더 (병합된 셀)
        col_start = 1  # A열은 부위명용
        for grade in self.grades:
            if grade in self.results:
                # 각 등급마다 4개 컬럼 (10%, 20%, 30%, 40% 마진만)
                grade_format = wb.add_format({
                    **base_format,
                    'bold': True, 'align': 'center',
                    'bg_color': grade_colors[grade], 'font_color': 'white', 'font_size': 11
                })
                ws.merge_range(1, col_start, 1, col_start + 3, f"{grade}급", grade_format)
                col_start += 4

        # Row 2: 컬럼 헤더
        ws.write(2, 0, "부위", fmt_col_header)
        col = 1
        for grade in self.grades:
            if grade in self.results:
                ws.write(2, col, "10%마진", fmt_col_header)
                ws.write(2, col + 1, "20%마진", fmt_col_header)
                ws.write(2, col + 2, "30%마진", fmt_col_header)
                ws.write(2, col + 3, "40%마진", fmt_col_header)
                col += 4

        # 모든 등급에서 공통 부위 목록 생성
        all_parts = set()
        for grade_df in self.results.values():
            all_parts.update(grade_df['부위'].tolist())

        # 부위 순서 정렬 (기존 순서 유지)
        if self.results:
            first_grade_df = next(iter(self.results.values()))
            ordered_parts = first_grade_df['부위'].tolist()
            # 다른 등급에만 있는 부위들 추가
            for part in sorted(all_parts):
                if part not in ordered_parts:
                    ordered_parts.append(part)
        else:
            ordered_parts = sorted(all_parts)

        # 데이터 행 생성 (수식 사용)
        for row_idx, part in enumerate(ordered_parts, start=3):
            ws.write(row_idx, 0, part, fmt_part_name)

            col = 1
            for grade in self.grades:
                if grade in self.results:
                    grade_df = self.results[grade]
                    part_rows = grade_df[grade_df['부위'] == part]

                    if not part_rows.empty:
                        # 해당 부위가 이 등급에 존재함 - 수식으로 참조
                        part_row_in_sheet = None
                        # 개별 등급 시트에서 이 부위의 행 번호 찾기
                        for sheet_row_idx, (_, sheet_row) in enumerate(grade_df.iterrows(), start=6):
                            if sheet_row['부위'] == part:
                                part_row_in_sheet = sheet_row_idx + 1  # Excel 행 번호 (1부터 시작)
                                break

                        if part_row_in_sheet:
                            # 수식으로 참조 (K~N열=금천10% 마진 10%, 20%, 30%, 40%)
                            ws.write_formula(row_idx, col, f"=ROUND('{grade}'!K{part_row_in_sheet},0)", fmt_number)
                            ws.write_formula(row_idx, col + 1, f"=ROUND('{grade}'!L{part_row_in_sheet},0)", fmt_number)
                            ws.write_formula(row_idx, col + 2, f"=ROUND('{grade}'!M{part_row_in_sheet},0)", fmt_number)
                            ws.write_formula(row_idx, col + 3, f"=ROUND('{grade}'!N{part_row_in_sheet},0)", fmt_number)
                    else:
                        # 해당 부위가 이 등급에 없음 - 빈 셀
                        empty_format = wb.add_format(base_format)
                        for i in range(4):
                            ws.write(row_idx, col + i, "", empty_format)

                    col += 4

        # 컬럼 너비 조정 (가로형태 고려)
        ws.set_column(0, 0, 12)  # 부위명
        for i in range(1, 17):
            ws.set_column(i, i, 9)  # 가격 컬럼들

        # 틀 고정
        ws.freeze_panes(3, 1)

        # 인쇄 설정
        ws.fit_to_pages(1, 0)  # 가로 1페이지에 맞춤

    def export_all_data_excel(self, filename=None):
        """All Data 형식 엑셀 저장 (pork margin all data 형식)"""
        if filename is None:
            filename = f"beef_margin_all_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        all_data = []
        today = datetime.now().strftime('%Y-%m-%d')

        for grade, df in self.results.items():
            if df.empty:
                continue

            for _, row in df.iterrows():
                part = row['부위']

                # 경락가 기반 - 적수원가
                all_data.append({
                    'date': today,
                    'source': '경락가기준(적수비방식)',
                    'type': '적수원가',
                    'Species': '한우',
                    'Part': part,
                    'Grade': grade,
                    'Price': int(row['경락가_적수원가(원/kg)']),
                    'Price_Per_Kg': f"{int(row['경락가_적수원가(원/kg)']):,}원"
                })

                # 경락가 기반 - 마진별 가격
                for margin_pct in [10, 20, 30, 40]:
                    col_name = f'경락가_{margin_pct}%마진'
                    all_data.append({
                        'date': today,
                        'source': '경락가기준(적수비방식)',
                        'type': f'{margin_pct}%마진',
                        'Species': '한우',
                        'Part': part,
                        'Grade': grade,
                        'Price': int(row[col_name]),
                        'Price_Per_Kg': f"{int(row[col_name]):,}원"
                    })

                # 금천10% - 적수원가
                all_data.append({
                    'date': today,
                    'source': '금천미트(10%마진가정)',
                    'type': '적수원가',
                    'Species': '한우',
                    'Part': part,
                    'Grade': grade,
                    'Price': int(row['금천10%_적수원가(원/kg)']),
                    'Price_Per_Kg': f"{int(row['금천10%_적수원가(원/kg)']):,}원"
                })

                # 금천10% - 마진별 가격
                for margin_pct in [10, 20, 30, 40]:
                    col_name = f'금천10%_{margin_pct}%마진'
                    all_data.append({
                        'date': today,
                        'source': '금천미트(10%마진가정)',
                        'type': f'{margin_pct}%마진',
                        'Species': '한우',
                        'Part': part,
                        'Grade': grade,
                        'Price': int(row[col_name]),
                        'Price_Per_Kg': f"{int(row[col_name]):,}원"
                    })

        # DataFrame 생성 및 저장
        result_df = pd.DataFrame(all_data)

        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            result_df.to_excel(writer, sheet_name='All_Data', index=False)

            # 워크시트 가져오기
            worksheet = writer.sheets['All_Data']

            # 컬럼 너비 조정
            worksheet.set_column('A:A', 12)  # date
            worksheet.set_column('B:B', 25)  # source
            worksheet.set_column('C:C', 12)  # type
            worksheet.set_column('D:D', 10)  # Species
            worksheet.set_column('E:E', 12)  # Part
            worksheet.set_column('F:F', 8)   # Grade
            worksheet.set_column('G:G', 12)  # Price
            worksheet.set_column('H:H', 15)  # Price_Per_Kg

        print(f"All Data Excel 결과 저장: {filename}")
        return filename


# ============================================================
# 통합 메인 함수
# ============================================================

async def main():
    def upload_to_google_drive(file_path):
    """생성된 파일을 구글 드라이브에 업로드"""
    try:
        creds_json = os.environ.get('GDRIVE_CREDENTIALS')
        folder_id = os.environ.get('GDRIVE_FOLDER_ID')
        if not creds_json or not folder_id:
            print(f"[업로드 건너뜀] 환경변수 없음 (로컬 실행시 정상)")
            return
        info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(info)
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {'name': os.path.basename(file_path), 'parents': [folder_id]}
        media = MediaFileUpload(file_path, resumable=True)
        print(f"구글 드라이브 업로드 중: {os.path.basename(file_path)}")
        result = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"업로드 완료! (파일ID: {result.get('id')})")
    except Exception as e:
        print(f"업로드 실패: {os.path.basename(file_path)} - {e}")
 
    print("=== 한우 가격 수집 + 마진 계산 통합 프로그램 ===")

    # API 키 로드 (환경변수 → api_key.txt → 하드코딩 순서, 기존 코드와 동일)
    service_key = None
    service_key = os.getenv('EKAPE_API_KEY')
    if not service_key:
        try:
            with open('api_key.txt', 'r', encoding='utf-8') as f:
                service_key = f.read().strip()
        except FileNotFoundError:
            pass
    if not service_key:
        service_key = "LFq9u3tNGZKe+rUDioG7t8YJ6kLegDAwuy6sKuZAEHWUQ2RnPHUdh70zsjagYIdCWLKvoyxP4My/320pPvCatw=="

    # ── 1단계: 가격 데이터 수집 ──
    print("\n[1단계] 가격 데이터 수집 중...")
    scraper = BeefCompleteScraper(service_key=service_key)
    auction_success = scraper.collect_auction_data()
    market_success = await scraper.collect_market_wholesale_data()

    if not (auction_success or market_success):
        print("데이터 수집 실패")
        return

    scraper.print_summary()

    # Excel 중간 파일 저장 (MarginCalculatorCompare가 읽을 파일)
    price_filename = f"beef_price_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    if not scraper.save_excel(filename=price_filename):
        print("가격 파일 저장 실패")
        return

    # ── 2단계: 마진 계산 ──
    print("\n[2단계] 적수원가/마진 계산 중...")
    calculator = MarginCalculatorCompare(price_filename)

    if not calculator.load_data():
        print("데이터 로드 실패")
        return
    if not calculator.prepare_data():
        print("데이터 전처리 실패")
        return
    if not calculator.generate_results():
        print("계산 실패")
        return

    html_file = calculator.export_html()
    excel_file = calculator.export_excel()
    all_data_file = calculator.export_all_data_excel()
 # 구글 드라이브 업로드
 for f in [price_filename, html_file, excel_file, all_data_file]:
if f:
upload_to_google_drive(f)

    print(f"\n=== 모든 작업 완료 ===")
    print(f"가격 데이터: {price_filename}")
    print(f"HTML: {html_file}")
    print(f"Excel (비교): {excel_file}")
    print(f"Excel (All Data): {all_data_file}")


# ============================================================
# 진입점 - Windows 더블클릭 실행 지원
# ============================================================

# 더블클릭으로 실행 시 cmd 창에서 UTF-8로 재실행
if __name__ == "__main__":
    if os.name == 'nt' and not os.environ.get('BEEF_ALL_RUNNING'):
        # Windows 로컬: 한글 깨짐 방지
        os.environ['BEEF_ALL_RUNNING'] = '1'
        os.system(f'cmd /k "chcp 65001 > nul && python "{__file__}""')
        sys.exit()
    else:
        # Linux(GitHub Actions) 또는 재실행된 Windows
        asyncio.run(main())
