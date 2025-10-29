import re
import pandas as pd
import unicodedata


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

# Cell 11: Filter NCE matching data
df_nce = merged[(merged['대학구분'] == '대학') & (merged['학교구분'] == '대학교') & (merged['본분교'] == '본교') & (merged['(대학)지역'] == '서울')
          & (merged['주야간구분'] == '주간') & (merged['학과특성'] == '일반과정') & (merged['학과상태'] != '폐지') & (merged['수업연한'] == '4년')].copy()

# 확률 구하기
#1: 데이터 불러오기 및 기본 데이터 구성
all_code = all_code.dropna(subset=['교육과정'])

df_nce = df_nce.replace('N.C.E.', 'N.C.E', regex=False)
df_nce = df_nce.drop(columns = ['Unnamed: 0'])

nce = df_nce[(df_nce['대계열분류'] == 'N.C.E') | (df_nce['중계열분류'] == 'N.C.E') | (df_nce['소계열분류'] == 'N.C.E') | (df_nce['대계열분류'] == '광역계열')]
no_nce = all_code[(all_code['대계열분류'] != 'N.C.E') & (all_code['중계열분류'] != 'N.C.E') & (all_code['소계열분류'] != 'N.C.E') & (all_code['소계열분류'] != 'N.C.E.')]
no_nce = no_nce[no_nce['교육과정'] != 'nan']
no_nce = no_nce.drop(columns = ['Unnamed: 0'])

#2: 교육과정별 대중소 계열별 비율 값
def get_course_distribution(no_nce):
    # ① 교육과정별, 대/중/소계열별 빈도 계산
    grouped = (
        no_nce
        .groupby(['교육과정', '대계열분류', '중계열분류', '소계열분류'])
        .size()
        .reset_index(name='count')
    )

    # ② 교육과정별 총 count 대비 비율 계산
    grouped['비율'] = (
        grouped.groupby('교육과정')['count']
        .transform(lambda x: x / x.sum())
    )
    return grouped

grouped = get_course_distribution(no_nce)

#3: 교육과정별 대중소 계열별 비율 값 불러오기
course_ratio = grouped.copy()
course_ratio = course_ratio.replace('N.C.E.', 'N.C.E', regex=False)

#4: 교육과정별 NCE 추천결과 생성
# 대중소 결합
course_ratio['추천_대중소'] = (
    course_ratio['대계열분류'].astype(str) + '-' +
    course_ratio['중계열분류'].astype(str) + '-' +
    course_ratio['소계열분류'].astype(str)
)

# NCE 대상만 필터링
target_courses = nce['교육과정'].unique()
filtered = course_ratio[course_ratio['교육과정'].isin(target_courses)].copy()

# 비율 기준 정렬 및 순위 부여
filtered = filtered.sort_values(['교육과정', '비율'], ascending=[True, False])
filtered['추천순위'] = filtered.groupby('교육과정').cumcount() + 1

# pivot 변환
pivoted = (
    filtered
    .pivot(index='교육과정', columns='추천순위', values=['추천_대중소', '비율'])
    .sort_index(axis=1, level=1)
)

# 컬럼 순서 및 이름 정리 (순위 → 확률 순서)
new_cols = []
max_rank = filtered['추천순위'].max()
for i in range(1, max_rank + 1):
    new_cols.append(('추천_대중소', i))
    new_cols.append(('비율', i))
pivoted = pivoted[new_cols]

# 컬럼명 변경: 대중소_1순위 / 대중소_1순위_확률 형태
pivoted.columns = [
    f"대중소_{col[1]}순위{'_확률' if col[0] == '비율' else ''}"
    for col in pivoted.columns
]

pivoted = pivoted.reset_index()


#5: 지역, 학교 등 필터링된 데이터와 교육과정별 대중소 순위 데이터 병합
# 2️⃣ 교육과정 기준 merge
merged = df_nce.merge(pivoted, how='left')

#6: 검증 단계에서 쓰일 데이터 저장: verified_merged
verified_merged = merged.copy()
# merged.to_excel('교육과정별_NCE_추천결과_20241022.xlsx', index=False) #검증 때 쓰임

#7: NCE 전공별 대중소 추천결과 생성
# nce 전공 목록만 추출
nce_keys = nce[['학교명', '학부·과(전공)명']].drop_duplicates()

# merged에서 nce 전공에 해당하는 데이터만 가져오기
all_code = merged.merge(nce_keys, on=['학교명', '학부·과(전공)명'], how='inner')
print(f"NCE 전공이 포함된 merged 행 수: {len(all_code)}")

# 표본수 숫자 변환
all_code['표본수 n'] = pd.to_numeric(all_code['표본수 n'], errors='coerce').fillna(1)

# 순위열 자동 인식
rank_cols = [c for c in all_code.columns if c.startswith('대중소_') and not c.endswith('_확률')]
prob_cols = [c for c in all_code.columns if c.startswith('대중소_') and c.endswith('_확률')]

print("인식된 순위 컬럼:", rank_cols)
print("인식된 확률 컬럼:", prob_cols)

# 순위별 long 변환
long_list = []
for i in range(1, len(rank_cols) + 1):
    cat_col = f'대중소_{i}순위'
    prob_col = f'대중소_{i}순위_확률'
    if cat_col in all_code.columns and prob_col in all_code.columns:
        sub = all_code[['학교명', '학부·과(전공)명', '교육과정', '표본수 n', cat_col, prob_col]].copy()
        sub.columns = ['학교명', '학부·과(전공)명', '교육과정', '표본수', '대중소', '확률']
        long_list.append(sub)

melted = pd.concat(long_list, ignore_index=True).dropna(subset=['대중소', '확률'])

# 교육과정별 가중 평균
agg = (
    melted.groupby(['학교명', '학부·과(전공)명', '교육과정', '대중소'], as_index=False)
    .apply(lambda g: (g['확률'] * g['표본수']).sum() / g['표본수'].sum())
    .reset_index()
)
agg.columns = ['_','학교명','학부·과(전공)명','교육과정','대중소','가중확률']
agg = agg.drop(columns='_')

# 전공별 대중소별 확률 합산
summed = (
    agg.groupby(['학교명', '학부·과(전공)명', '대중소'], as_index=False)['가중확률']
    .sum()
)

# 전공별 확률 정규화 (총합 = 1)
summed['정규화확률'] = summed.groupby(['학교명', '학부·과(전공)명'])['가중확률']\
    .transform(lambda x: x / x.sum())

# 전공별 전체 순위 부여 (1~N순위)
summed['순위'] = summed.groupby(['학교명', '학부·과(전공)명'])['정규화확률']\
    .rank(method='first', ascending=False).astype(int)

# wide 형태로 pivot (전공별 순위별 추천 결과 모두 표시)
pivoted = (
    summed
    .pivot(index=['학교명', '학부·과(전공)명'], columns='순위', values=['대중소', '정규화확률'])
    .sort_index(axis=1, level=1)
)

# 컬럼명 정리
new_cols = []
max_rank = summed['순위'].max()
for i in range(1, max_rank + 1):
    new_cols.append(('대중소', i))
    new_cols.append(('정규화확률', i))
pivoted = pivoted[new_cols]

pivoted.columns = [
    f"추천_대중소_{col[1]}순위" if col[0] == '대중소' else f"추천_확률_{col[1]}순위"
    for col in pivoted.columns
]
pivoted = pivoted.reset_index()

# 결과
course_ratio_result_nce = pivoted.copy()

#8: NCE 전공별 대중소 추천결과 저장
# course_ratio_result_nce.to_excel('C:\\Users\\heeyo\\Desktop\\KERIS_2종\\nce_course_ratio_recommend.xlsx', index = False)
course_ratio_result_nce
