#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
한우 가격 수집 + 마진 계산 통합 실행 스크립트

실행 순서:
  1단계: beef_selector_clean.py - 금천미트 시장가 + 경락가 수집 -> beef_price_*.xlsx 생성
  2단계: marginb_compare.py     - 위 파일을 읽어 적수원가/마진 계산 -> 결과 파일 생성
"""

import sys
import os

# 더블클릭 실행 시 cmd 창에서 UTF-8로 재실행 (한글 출력 보장)
if __name__ == "__main__" and not os.environ.get('RUN_BEEF_MARGIN'):
    os.environ['RUN_BEEF_MARGIN'] = '1'
    os.system(f'cmd /k "chcp 65001 > nul && python "{__file__}""')
    sys.exit()

import subprocess
import glob
from datetime import datetime


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def run_script(script_name, extra_env=None):
    """스크립트를 서브프로세스로 실행하고 실시간으로 출력을 보여줌."""
    script_path = os.path.join(SCRIPT_DIR, script_name)
    if not os.path.exists(script_path):
        print(f"[오류] 파일을 찾을 수 없습니다: {script_path}")
        return False

    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    proc = subprocess.Popen(
        [sys.executable, script_path],
        env=env,
        cwd=SCRIPT_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace',
        bufsize=1,
    )

    for line in proc.stdout:
        print(line, end='', flush=True)

    proc.wait()
    return proc.returncode == 0


def find_latest_beef_price_file():
    """가장 최근에 생성된 beef_price_*.xlsx 파일 경로 반환."""
    pattern = os.path.join(SCRIPT_DIR, 'beef_price_*.xlsx')
    files = glob.glob(pattern)
    if not files:
        return None
    return max(files, key=os.path.getmtime)


def main():
    print("=" * 60)
    print("  한우 가격 수집 + 마진 계산 통합 실행")
    print(f"  시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    os.chdir(SCRIPT_DIR)

    # ── 1단계: 가격 데이터 수집 ──────────────────────────────
    print("\n[1단계] beef_selector_clean.py - 가격 데이터 수집")
    print("-" * 60)

    files_before = set(glob.glob(os.path.join(SCRIPT_DIR, 'beef_price_*.xlsx')))

    success1 = run_script('beef_selector_clean.py')

    if not success1:
        print("\n[오류] 1단계(가격 수집)가 실패했습니다. 프로그램을 종료합니다.")
        input("\n엔터를 누르면 종료됩니다...")
        sys.exit(1)

    # 새로 생성된 파일 확인
    files_after = set(glob.glob(os.path.join(SCRIPT_DIR, 'beef_price_*.xlsx')))
    new_files = files_after - files_before
    if new_files:
        new_file = list(new_files)[0]
        print(f"\n[완료] 가격 파일 생성: {os.path.basename(new_file)}")
    else:
        latest = find_latest_beef_price_file()
        if latest:
            print(f"\n[확인] 기존 가격 파일 사용: {os.path.basename(latest)}")
        else:
            print("\n[오류] beef_price_*.xlsx 파일이 없습니다. 프로그램을 종료합니다.")
            input("\n엔터를 누르면 종료됩니다...")
            sys.exit(1)

    # ── 2단계: 마진 계산 ────────────────────────────────────
    print("\n[2단계] marginb_compare.py - 적수원가/마진 계산")
    print("-" * 60)

    # MARGINB_RUNNING=1 : marginb_compare.py의 더블클릭 재실행 가드를 우회
    success2 = run_script('marginb_compare.py', extra_env={'MARGINB_RUNNING': '1'})

    print("\n" + "=" * 60)
    if success2:
        print("  모든 작업이 완료되었습니다!")
    else:
        print("  [경고] 2단계(마진 계산)에서 오류가 발생했습니다.")
    print(f"  완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    input("\n엔터를 누르면 종료됩니다...")


if __name__ == "__main__":
    main()
