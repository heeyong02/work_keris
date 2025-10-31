import re
import unicodedata
from io import BytesIO
from datetime import datetime

import pandas as pd
import streamlit as st
from loguru import logger

# 페이지 설정
st.set_page_config(
    page_title="NCE 전공 분류 추천 시스템", page_icon="🏫", layout="wide"
)

# 제목
st.title("🏫 NCE 전공 표준분류체계 추천 시스템")
st.markdown("---")

# 사이드바
st.sidebar.header("📂 데이터 업로드")


# === 함수 정의 ===
# 전각→반각 정규화
def nfkc(x):
    return unicodedata.normalize("NFKC", str(x)) if pd.notna(x) else x


# eng→kor 사전 생성
def build_eng2kor(series):
    eng2kor = {}
    norm = series.dropna().map(nfkc).unique().tolist()
    for c in norm:
        c_std = c.replace("(", "(").replace(")", ")")
        kor = re.sub(r"\(.*?\)", "", c_std).strip()
        inside = re.findall(r"\((.*?)\)", c_std)
        if inside:
            inside_text = inside[0].strip()
            if re.search(r"[A-Za-z]", inside_text):
                eng = re.sub(r"\d+([\-\.]\d+)?$", "", inside_text).strip()
                eng2kor[eng.lower()] = kor
    return eng2kor


# 과목 정제 함수
def clean_course(name, eng2kor=None):
    if pd.isna(name):
        return None

    raw = nfkc(name).strip()

    if eng2kor:
        raw_lower = raw.lower()
        for eng, kor in eng2kor.items():
            if eng and eng in raw_lower:
                return kor

    txt = raw
    txt = re.sub(r"\[.*?\]", "", txt)

    while re.search(r"\([^()]*\)", txt):
        txt = re.sub(r"\([^()]*\)", "", txt)
        txt = re.sub(r"(?<=[가-힣A-Za-z])[\-\.]?\d+$", "", txt)

    txt = re.sub(r"[ｏ@#☆ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]", "", txt)
    txt = re.sub(r"(?<=[가-힣])(I|II|III|IV|V|VI|VII|VIII|IX|X)$", "", txt)
    txt = re.sub(r"(?<=[가-힣A-Za-z])\d+(?=[가-힣A-Za-z])", "", txt)
    txt = re.sub(r"(?<=[가-힣A-Za-z])\d+$", "", txt).strip()
    txt = re.sub(r"\s+", " ", txt).strip()
    txt = re.sub(r"\s", "", txt).strip()

    if txt in ["", "/", "-", "--", "NULL", "NaN", "없음"]:
        return None
    return txt


# 숫자 버전 과목 통합 함수
def remove_number_if_duplicate(df, col="교육과정"):
    df["비교용"] = df[col].str.replace(r"\d+", "", regex=True)
    dup_list = df["비교용"].value_counts()
    dup_list = dup_list[dup_list > 1].index.tolist()
    df[col] = df.apply(
        lambda row: re.sub(r"\d+", "", row[col])
        if row["비교용"] in dup_list
        else row[col],
        axis=1,
    )
    df.drop(columns="비교용", inplace=True)
    return df


# 교육과정별 대중소 계열별 비율 계산
@st.cache_data
def get_course_distribution(no_nce):
    """
    교육과정별 대중소 계열별 비율을 계산하는 함수

    Args:
        no_nce: NCE가 아닌 데이터프레임

    Returns:
        pd.DataFrame: 교육과정별 분류 비율이 계산된 데이터프레임
    """
    grouped = (
        no_nce.groupby(["교육과정", "대계열분류", "중계열분류", "소계열분류"])
        .size()
        .reset_index(name="count")
    )
    grouped["비율"] = grouped.groupby("교육과정")["count"].transform(
        lambda x: x / x.sum()
    )
    return grouped

@st.cache_data
def load_file_by_extension(file, skiprows=None):
    """
    파일 확장자에 따라 적절한 방법으로 파일을 로드하는 함수

    Args:
        file: 업로드된 파일 객체
        skiprows: Excel 파일의 경우 건너뛸 행 수

    Returns:
        pd.DataFrame: 로드된 데이터프레임

    Raises:
        ValueError: 지원하지 않는 파일 형식인 경우
    """
    file_name = file.name.lower()

    if file_name.endswith(".parquet"):
        return pd.read_parquet(file)
    elif file_name.endswith((".xlsx", ".xls")):
        if skiprows is not None:
            return pd.read_excel(file, skiprows=skiprows)
        else:
            return pd.read_excel(file)
    else:
        raise ValueError(f"지원하지 않는 파일 형식입니다: {file_name}")


# === 파일 업로드 ===
def upload_file(label, key):
    """
    Streamlit sidebar에서 파일 업로드 위젯을 생성하는 함수.
    Excel, Parquet, CSV 파일을 지원합니다.
    """
    return st.sidebar.file_uploader(
        label,
        type=["xlsx", "parquet", "csv"],
        key=key
    )

uploaded_file1 = upload_file("학교별 교육편제단위 정보 파일 (Excel/Parquet/csv)", "file1")
uploaded_file2 = upload_file("교육과정_대학 파일 (Excel/Parquet/csv)", "file2")

# === session_state 초기화 ===
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'results' not in st.session_state:
    st.session_state.results = None

# === 업로드 완료 버튼 추가 ===
load_button = st.sidebar.button("📤 데이터 업로드 완료")

# 버튼이 눌리면 데이터 로드 시작
if load_button and uploaded_file1 and uploaded_file2:
    st.session_state.data_loaded = False  # 재처리 시작

# === 데이터 처리 및 결과 표시 ===
if load_button and uploaded_file1 and uploaded_file2:
    if not st.session_state.data_loaded:
        try:
            data_loading_start_time = datetime.now()
            with st.spinner("📊 데이터를 로딩하는 중..."):
                # 데이터 로드
                df1 = load_file_by_extension(uploaded_file1, skiprows=4)
                df2 = load_file_by_extension(uploaded_file2, skiprows=8)
                st.success("✅ 데이터 로딩 완료!")

            # 이후 Step 1 ~ Step 7 전부 여기에 포함!
            data_loading_time = datetime.now() - data_loading_start_time
            logger.info(f"데이터 로딩 완료 시간: {data_loading_time}")

            # 진행 상황 표시
            progress_bar = st.progress(0, text="데이터 처리 중...")

            # Step 1: 컬럼 이름 변경
            data_processing_start_time = datetime.now()
            progress_bar.progress(10, text="1/7 컬럼 이름 변경 중...")

            df2 = df2.rename(
                columns={
                    "차수": "조사차수",
                    "본분교명": "대학구분",
                    "학교구분": "본분교",
                    "학부·과(전공)코드": "학교별학과코드",
                    "주야구분명": "주야간구분",
                    "학부특성명": "학과특성",
                }
            )

            column_name_change_time = datetime.now() - data_processing_start_time
            logger.info(f"컬럼 이름 변경 완료 시간: {column_name_change_time}")

            # Step 2: 데이터 병합
            progress_bar.progress(20, text="2/7 데이터 전처리 및 병합 중...")

            data_processing_start_time = datetime.now()
            for df in [df1, df2]:
                df["학교코드"] = df["학교코드"].astype(str).str.strip()
                df["학교별학과코드"] = df["학교별학과코드"].astype(str).str.strip()
                df["학부·과(전공)명"] = (
                    df["학부·과(전공)명"].astype(str).str.replace(r"\s+", "", regex=True)
                )

            attach_cols = ["교육과정", "이수구분", "학점", "교과목해설"]
            df2_slim = df2[
                ["학교코드", "학교별학과코드", "학부·과(전공)명"] + attach_cols
            ].copy()

            merged = pd.merge(
                df1,
                df2_slim,
                on=["학교코드", "학교별학과코드", "학부·과(전공)명"],
                how="left",
                validate="1:m",
            )

            data_processing_time = datetime.now() - data_processing_start_time
            logger.info(f"데이터 처리 완료 시간: {data_processing_time}")

            # Step 3: 교육과정 정제
            progress_bar.progress(40, text="3/7 교육과정 이름 정제 중...")
            education_program_correction_start_time = datetime.now()

            # 교육과정 정제 적용
            eng2kor = build_eng2kor(merged["교육과정"])
            merged["교육과정"] = merged["교육과정"].apply(
                lambda x: clean_course(x, eng2kor)
            )
            merged = merged.dropna(subset=["교육과정"])

            # 숫자 버전 중복 제거
            merged = remove_number_if_duplicate(merged, col="교육과정")

            merged["표본수 n"] = (
                merged.groupby("교육과정")["교육과정"].transform("count").astype("Int64")
            )

            education_program_correction_time = (
                datetime.now() - education_program_correction_start_time
            )
            logger.info(
                f"교육과정 이름 정제 완료 시간: {education_program_correction_time}"
            )

            all_code = merged.copy()

            # Step 4: NCE 필터링
            progress_bar.progress(50, text="4/7 NCE 데이터 필터링 중...")
            nce_filtering_start_time = datetime.now()
            df_nce = merged[
                (merged["대학구분"] == "대학")
                & (merged["학교구분"] == "대학교")
                & (merged["본분교"] == "본교")
                & (merged["(대학)지역"] == "서울")
                & (merged["주야간구분"] == "주간")
                & (merged["학과특성"] == "일반과정")
                & (merged["학과상태"] != "폐지")
                & (merged["수업연한"] == "4년")
            ].copy()
            nce_filtering_time = datetime.now() - nce_filtering_start_time
            logger.info(f"NCE 필터링 완료 시간: {nce_filtering_time}")

            all_code = all_code.dropna(subset=["교육과정"])
            df_nce = df_nce.replace("N.C.E.", "N.C.E", regex=False)
            df_nce = df_nce.drop(columns=["Unnamed: 0"], errors="ignore")

            nce = df_nce[
                (df_nce["대계열분류"] == "N.C.E")
                | (df_nce["중계열분류"] == "N.C.E")
                | (df_nce["소계열분류"] == "N.C.E")
                | (df_nce["대계열분류"] == "광역계열")
            ]

            no_nce = all_code[
                (all_code["대계열분류"] != "N.C.E")
                & (all_code["중계열분류"] != "N.C.E")
                & (all_code["소계열분류"] != "N.C.E")
                & (all_code["소계열분류"] != "N.C.E.")
            ]
            no_nce = no_nce[no_nce["교육과정"] != "nan"]
            no_nce = no_nce.drop(columns=["Unnamed: 0"], errors="ignore")

            # Step 5: 교육과정별 비율 계산
            progress_bar.progress(60, text="5/7 교육과정별 분류 비율 계산 중...")
            course_distribution_calculation_start_time = datetime.now()
            grouped = get_course_distribution(no_nce)
            course_ratio = grouped.copy()
            course_ratio = course_ratio.replace("N.C.E.", "N.C.E", regex=False)
            course_distribution_calculation_time = (
                datetime.now() - course_distribution_calculation_start_time
            )
            logger.info(
                f"교육과정별 분류 비율 계산 완료 시간: {course_distribution_calculation_time}"
            )

            # Step 6: 추천 결과 생성
            progress_bar.progress(75, text="6/7 추천 결과 생성 중...")
            recommendation_result_generation_start_time = datetime.now()
            course_ratio["추천_대중소"] = (
                course_ratio["대계열분류"].astype(str)
                + "-"
                + course_ratio["중계열분류"].astype(str)
                + "-"
                + course_ratio["소계열분류"].astype(str)
            )

            target_courses = nce["교육과정"].unique()
            filtered = course_ratio[course_ratio["교육과정"].isin(target_courses)].copy()
            filtered = filtered.sort_values(["교육과정", "비율"], ascending=[True, False])
            filtered["추천순위"] = filtered.groupby("교육과정").cumcount() + 1

            pivoted = filtered.pivot(
                index="교육과정", columns="추천순위", values=["추천_대중소", "비율"]
            ).sort_index(axis=1, level=1)

            new_cols = []
            max_rank = filtered["추천순위"].max()
            for i in range(1, max_rank + 1):
                new_cols.append(("추천_대중소", i))
                new_cols.append(("비율", i))
            pivoted = pivoted[new_cols]

            pivoted.columns = [
                f"대중소_{col[1]}순위{'_확률' if col[0] == '비율' else ''}"
                for col in pivoted.columns
            ]
            pivoted = pivoted.reset_index()

            merged_result = df_nce.merge(pivoted, how="left")

            recommendation_result_generation_time = (
                datetime.now() - recommendation_result_generation_start_time
            )
            logger.info(
                f"추천 결과 생성 완료 시간: {recommendation_result_generation_time}"
            )

            # Step 7: 전공별 추천 생성
            progress_bar.progress(90, text="7/7 전공별 추천 결과 생성 중...")
            major_recommendation_generation_start_time = datetime.now()
            nce_keys = nce[["학교명", "학부·과(전공)명"]].drop_duplicates()
            all_code_final = merged_result.merge(
                nce_keys, on=["학교명", "학부·과(전공)명"], how="inner"
            )

            all_code_final["표본수 n"] = pd.to_numeric(
                all_code_final["표본수 n"], errors="coerce"
            ).fillna(1)

            rank_cols = [
                c
                for c in all_code_final.columns
                if c.startswith("대중소_") and not c.endswith("_확률")
            ]
            prob_cols = [
                c
                for c in all_code_final.columns
                if c.startswith("대중소_") and c.endswith("_확률")
            ]

            long_list = []
            for i in range(1, len(rank_cols) + 1):
                cat_col = f"대중소_{i}순위"
                prob_col = f"대중소_{i}순위_확률"
                if cat_col in all_code_final.columns and prob_col in all_code_final.columns:
                    sub = all_code_final[
                        [
                            "학교명",
                            "학부·과(전공)명",
                            "교육과정",
                            "표본수 n",
                            cat_col,
                            prob_col,
                        ]
                    ].copy()
                    sub.columns = [
                        "학교명",
                        "학부·과(전공)명",
                        "교육과정",
                        "표본수",
                        "대중소",
                        "확률",
                    ]
                    long_list.append(sub)

            melted = pd.concat(long_list, ignore_index=True).dropna(
                subset=["대중소", "확률"]
            )

            # 벡터화된 연산으로 최적화 (apply 대신)
            melted['가중값'] = melted['확률'] * melted['표본수']

            grouped = melted.groupby(
                ["학교명", "학부·과(전공)명", "교육과정", "대중소"]
            ).agg({
                '가중값': 'sum',
                '표본수': 'sum'
            }).reset_index()

            grouped['가중확률'] = grouped['가중값'] / grouped['표본수']
            agg = grouped[["학교명", "학부·과(전공)명", "교육과정", "대중소", "가중확률"]].copy()

            summed = agg.groupby(["학교명", "학부·과(전공)명", "대중소"], as_index=False)[
                "가중확률"
            ].sum()

            summed["정규화확률"] = summed.groupby(["학교명", "학부·과(전공)명"])[
                "가중확률"
            ].transform(lambda x: x / x.sum())

            summed["순위"] = (
                summed.groupby(["학교명", "학부·과(전공)명"])["정규화확률"]
                .rank(method="first", ascending=False)
                .astype(int)
            )

            pivoted_final = summed.pivot(
                index=["학교명", "학부·과(전공)명"],
                columns="순위",
                values=["대중소", "정규화확률"],
            ).sort_index(axis=1, level=1)

            new_cols = []
            max_rank = summed["순위"].max()
            for i in range(1, max_rank + 1):
                new_cols.append(("대중소", i))
                new_cols.append(("정규화확률", i))
            pivoted_final = pivoted_final[new_cols]

            pivoted_final.columns = [
                f"추천_대중소_{col[1]}순위"
                if col[0] == "대중소"
                else f"추천_확률_{col[1]}순위"
                for col in pivoted_final.columns
            ]
            pivoted_final = pivoted_final.reset_index()

            course_ratio_result_nce = pivoted_final.copy()

            major_recommendation_generation_time = (
                datetime.now() - major_recommendation_generation_start_time
            )
            logger.info(
                f"전공별 추천 결과 생성 완료 시간: {major_recommendation_generation_time}"
            )
            logger.info(f"전체 처리 완료 시간: {datetime.now() - data_loading_start_time}")

            progress_bar.progress(100, text="✅ 처리 완료!")

            # 결과를 session_state에 저장
            st.session_state.results = {
                'course_ratio_result_nce': course_ratio_result_nce,
                'merged_result': merged_result,
                'nce_keys': nce_keys,
                'nce': nce,
                'max_rank': max_rank
            }
            st.session_state.data_loaded = True

            # 성공 메시지
            st.success("✅ 모든 처리가 완료되었습니다! 아래에서 결과를 확인하세요.")

        except Exception as e:
            st.error(f"❌ 오류 발생: {str(e)}")
            st.exception(e)
            st.session_state.data_loaded = False
            st.stop()  # 에러 발생 시 여기서 멈춤

# 결과 표시 (데이터가 로드된 경우)
if st.session_state.data_loaded and st.session_state.results:
    results = st.session_state.results
    course_ratio_result_nce = results['course_ratio_result_nce']
    merged_result = results['merged_result']
    nce_keys = results['nce_keys']
    nce = results['nce']
    max_rank = results['max_rank']

    # === 결과 표시 ===
    st.markdown("---")
    st.header("📊 분석 결과")

    # 탭으로 구분
    tab1, tab2, tab3 = st.tabs(
        ["📈 통계 정보", "🎯 전공별 추천 결과", "📥 데이터 다운로드"]
    )

    with tab1:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("전체 전공 수", f"{len(nce_keys):,}개")
        with col2:
            st.metric("NCE 과목 수", f"{len(nce):,}개")
        with col3:
            st.metric("추천 결과 생성", f"{len(course_ratio_result_nce):,}건")
        with col4:
            st.metric("최대 추천 순위", f"{max_rank}순위")

        st.markdown("### 대계열 분포")
        major_dist = nce["대계열분류"].value_counts()
        st.bar_chart(major_dist)

    with tab2:
        st.markdown("### 전공별 표준분류체계 추천 결과")

        # 상위 5개 순위만 표시 (고정)
        num_ranks = 5
        display_cols = ["학교명", "학부·과(전공)명"]
        for i in range(1, num_ranks + 1):
            display_cols.append(f"추천_대중소_{i}순위")
            display_cols.append(f"추천_확률_{i}순위")

        display_cols = [col for col in display_cols if col in course_ratio_result_nce.columns]

        st.dataframe(course_ratio_result_nce[display_cols], use_container_width=True, height=500)

        st.info(f"📌 총 {len(course_ratio_result_nce)}개 전공의 추천 결과 (상위 5순위 표시)")

    with tab3:
        st.markdown("### 결과 다운로드")

        # Excel 다운로드 함수
        def to_excel(df):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="추천결과")
            return output.getvalue()

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 전공별 추천 결과")
            excel_data1 = to_excel(course_ratio_result_nce)
            st.download_button(
                label="📥 전공별 Excel 다운로드",
                data=excel_data1,
                file_name="nce_전공별_추천결과.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.caption(f"📊 {len(course_ratio_result_nce):,}개 전공")

        with col2:
            st.markdown("#### 교육과정별 추천 결과")
            excel_data2 = to_excel(merged_result)
            st.download_button(
                label="📥 교육과정별 Excel 다운로드",
                data=excel_data2,
                file_name="nce_교육과정별_추천결과.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            st.caption(f"📊 {len(merged_result):,}개 교육과정")

elif not st.session_state.data_loaded and uploaded_file1 and uploaded_file2:
    st.info("📥 두 파일을 업로드한 후, **'데이터 업로드 완료' 버튼**을 눌러주세요.")
else:
    st.info("👈 사이드바에서 필요한 파일을 업로드해주세요.")

    st.markdown("""
    ### 📋 사용 방법

    1. **파일 준비**
       - `학교별 교육편제단위 정보_YYYYMMDD기준.xlsx`
       - `교육과정_대학(YYYYMMDD).xlsx`

    2. **파일 업로드**
       - 좌측 사이드바에서 두 개의 Excel 파일을 업로드합니다.

    3. **결과 확인**
       - '데이터 업로드 완료' 클릭 시 데이터를 처리하고 결과를 표시합니다.
       - 학교별, 전공별 필터링이 가능합니다.

    4. **결과 다운로드**
       - 처리된 결과를 Excel 파일로 다운로드할 수 있습니다.

    ### 📊 주요 기능

    - ✅ NCE(분류 불가) 전공에 대한 표준분류체계 자동 추천
    - ✅ 교육과정 기반 확률 계산
    - ✅ 전공별 순위별 추천 결과 제공
    - ✅ 학교별 필터링 및 시각화
    - ✅ Excel 파일 다운로드
    """)

# 푸터
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: gray;'>"
    "NCE 전공 표준분류체계 추천 시스템 v1.0"
    "</div>",
    unsafe_allow_html=True,
)