import re
import pandas as pd
import unicodedata

# Cell 1: Import libraries and check versions
print(pd.__version__)
print(re.__version__)

# Cell 2: Load data
df1 = pd.read_excel('학교별 교육편제단위 정보_20251002기준.xlsx', skiprows=4)
df2 = pd.read_excel('교육과정_대학(20251020).xlsx', skiprows=8)

# Cell 3: Rename columns
df2 = df2.rename(columns={
    "차수": "조사차수",
    "본분교명": "대학구분",
    "학교구분": "본분교",
    "학부·과(전공)코드": "학교별학과코드",
    "주야구분명": "주야간구분",
    "학부특성명": "학과특성",
})

# Cell 4: Merge data
# 1️⃣ 키 컬럼 전처리 (공백/괄호 제거, 타입 통일)
for df in [df1, df2]:
    df['학교코드'] = df['학교코드'].astype(str).str.strip()
    df['학교별학과코드'] = df['학교별학과코드'].astype(str).str.strip()
    df['학부·과(전공)명'] = (
        df['학부·과(전공)명']
        .astype(str)
        .str.replace(r'\s+', '', regex=True)       # 모든 공백 제거
    )

# 2️⃣ df2에서 필요한 열만 선택
attach_cols = ['교육과정', '이수구분', '학점', '교과목해설']
df2_slim = df2[['학교코드', '학교별학과코드', '학부·과(전공)명'] + attach_cols].copy()

# 3️⃣ 조인
merged = pd.merge(
    df1,
    df2_slim,
    on=['학교코드', '학교별학과코드', '학부·과(전공)명'],
    how='left',      # 학과정보(df1)를 기준으로
    validate='1:m'   # 한 학과에 여러 과목이 붙는 구조
)

# Cell 5: Save merged data
# merged.to_excel('C:\\Users\\heeyo\\Desktop\\KERIS_2종\\교육편제단위_표준분류체계 결합_v2.xlsx', index=False)

# Cell 6: Define cleaning functions
# 0️⃣ 전각→반각 정규화
def nfkc(x):
    return unicodedata.normalize('NFKC', str(x)) if pd.notna(x) else x

# 1️⃣ eng→kor 사전 생성
def build_eng2kor(series):
    eng2kor = {}
    norm = series.dropna().map(nfkc).unique().tolist()
    for c in norm:
        c_std = c.replace('(', '(').replace(')', ')')
        kor = re.sub(r'\(.*?\)', '', c_std).strip()
        inside = re.findall(r'\((.*?)\)', c_std)
        if inside:
            inside_text = inside[0].strip()
            if re.search(r'[A-Za-z]', inside_text):
                eng = re.sub(r'\d+([\-\.]\d+)?$', '', inside_text).strip()
                eng2kor[eng.lower()] = kor
    return eng2kor


# 2️⃣ 과목 정제 함수 (3D모델링 안전 버전)
def clean_course(name, eng2kor=None):
    if pd.isna(name):
        return None

    # (A) 전각→반각 정규화
    raw = nfkc(name).strip()

    # (B) 영어→한글 매핑
    if eng2kor:
        raw_lower = raw.lower()
        for eng, kor in eng2kor.items():
            if eng and eng in raw_lower:
                return kor  # 매핑 우선 적용

    # (C) 나머지 정제
    txt = raw
    txt = re.sub(r'\[.*?\]', '', txt)  # 대괄호 제거

    # 중첩 괄호 제거 + 끝의 숫자 제거
    while re.search(r'\([^()]*\)', txt):
        txt = re.sub(r'\([^()]*\)', '', txt)
        # 끝 숫자 제거는 한글/영문자 바로 뒤 숫자만 대상으로 함
        txt = re.sub(r'(?<=[가-힣A-Za-z])[\-\.]?\d+$', '', txt)

    # 로마숫자 및 특수문자 제거
    txt = re.sub(r'[ｏ@#☆ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]', '', txt)

    # 한글 뒤에 붙은 영문 로마숫자(I~X) 제거 (예: 건축설계스튜디오I → 건축설계스튜디오)
    txt = re.sub(r'(?<=[가-힣])(I|II|III|IV|V|VI|VII|VIII|IX|X)$', '', txt)

    # 중간 숫자 제거 ('공업생화학1및실습' → '공업생화학및실습')
    txt = re.sub(r'(?<=[가-힣A-Za-z])\d+(?=[가-힣A-Za-z])', '', txt)

    # 끝의 숫자 제거 (한글·영문 뒤 숫자만, '3D모델링'은 그대로)
    txt = re.sub(r'(?<=[가-힣A-Za-z])\d+$', '', txt).strip()

    # 공백 정리
    txt = re.sub(r'\s+', ' ', txt).strip()
    txt = re.sub(r'\s', '', txt).strip()

    # 무의미한 문자열 처리
    if txt in ['', '/', '-', '--', 'NULL', 'NaN', '없음']:
        return None
    return txt


# 3️⃣ 숫자 버전 과목 통합 함수
def remove_number_if_duplicate(df, col='교육과정', verbose=True):
    """
    동일 과목명에서 숫자만 다른 경우를 통합
    (예: '공업생화학1및실습' + '공업생화학2및실습' → '공업생화학및실습')
    유일한 숫자 과목은 유지.
    """
    before = df[col].copy()

    # 비교용: 숫자 제거 버전
    df['비교용'] = df[col].str.replace(r'\d+', '', regex=True)

    # 숫자 제거 후 중복 존재하는 패턴만 식별
    dup_list = df['비교용'].value_counts()
    dup_list = dup_list[dup_list > 1].index.tolist()

    # 중복 패턴이면 숫자 제거, 아니면 그대로 둠
    df[col] = df.apply(
        lambda row: re.sub(r'\d+', '', row[col]) if row['비교용'] in dup_list else row[col],
        axis=1
    )

    # 임시 컬럼 삭제
    df.drop(columns='비교용', inplace=True)
    return df

# Cell 9: Add sample count
merged['표본수 n'] = (
    merged.groupby('교육과정')['교육과정'].transform('count').astype('Int64')
)

# Cell 10: Save final merged data
all_code = merged.copy()
# all_code.to_excel('과정 전체 데이터_20251022.xlsx')
all_code
# Cell 11: Filter NCE matching data
df_nce = merged[(merged['대학구분'] == '대학') & (merged['학교구분'] == '대학교') & (merged['본분교'] == '본교') & (merged['(대학)지역'] == '서울')
          & (merged['주야간구분'] == '주간') & (merged['학과특성'] == '일반과정') & (merged['학과상태'] != '폐지') & (merged['수업연한'] == '4년')].copy()
df_nce
# Cell 12: Save NCE matching data
#df_nce.to_excel('nce매칭 필요 데이터_20251022.xlsx')