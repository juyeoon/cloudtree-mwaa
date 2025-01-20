import chardet  # 문자 인코딩을 감지하는 데 사용되는 라이브러리
import pandas as pd  # 데이터 분석 및 처리를 위한 라이브러리

"""
CSV 파일의 문자 인코딩을 감지
"""

# 확인할 CSV 파일의 이름을 지정
filename = "city_park_2.csv"

# 파일을 바이너리 읽기 모드('rb')로 열기
with open(filename, 'rb') as f:
    # 파일의 첫 번째 줄을 읽고, chardet을 사용해 인코딩을 감지
    result = chardet.detect(f.readline())
    # 감지된 인코딩 정보를 출력
    print(result['encoding'])
