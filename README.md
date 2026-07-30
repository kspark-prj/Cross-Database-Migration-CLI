# VeloxDB Cross-DB 마이그레이션 CLI 도구

VeloxDB는 Python 기반으로 작성된 초고속, OOM 방지(Out-Of-Memory Safe), 락 프리(Lock-Free) 교차 데이터베이스(Cross-DB) 마이그레이션 CLI 프로그램입니다. 소스 데이터베이스와 대상 데이터베이스가 서로 물리적으로 분리되어 있거나 동시 접속이 엄격히 통제되는 망분리 환경에서 100GB 이상의 초대용량 데이터베이스(수억 ~ 수십억 건의 레코드)를 OOM이나 시스템 다운 없이 안정적이고 신속하게 처리할 수 있도록 개발되었습니다.

---

<img src="main.png" width="800" height="300">

## 🚀 주요 특징 및 성능 최적화 기술

### 1. 물리적 단계 격리 (Phase-based Separation)

- **Phase 1 (Extract - 소스 DB 접속)**: 소스 DB 스키마 구조, 인덱스, FK 등 메타데이터를 추출하고 데이터를 청크 단위 Parquet 파일로 압축 덤프합니다. 정합성 검증용 오프라인 통계 파일(`source_checksums.json`)을 생성합니다.
- **Phase 2 (Load & Validate - 대상 DB 접속)**: 소스 DB 접속 없이 덤프된 자산만을 사용하여 대상 DB에 스키마를 적용하고, 데이터를 고속 적재한 후 오프라인 상태에서 정합성을 정밀 검증합니다.

### 2. DuckDB C++ 엔진 & Native Driver 하이브리드 아키텍처

최고 수준의 I/O 처리 및 적재 속도를 위해 DuckDB의 C++ 엔진 및 native 커넥터를 메인 파이프라인으로 채택하고, 환경에 따른 드라이버 미지원 문제에 대응하기 위해 Native DBAPI Fallback을 결합했습니다.

- **Phase 1 고속 덤프**: PostgreSQL 및 MySQL 환경에서 DuckDB의 C++ 확장 기능(`postgres`, `mysql`)을 이용해 소스 DB를 직접 마운트(`ATTACH`)한 뒤, `COPY ... TO ... (FORMAT 'parquet')` 명령으로 Python 런타임 메모리를 거치지 않고 직접 Parquet 파일로 고속 추출합니다.
- **Phase 2 단일 파일 정밀 적재 (DuckDB Bulk Insert)**: DuckDB 대상 DB 마운트 및 `INSERT INTO target_db.table SELECT * FROM read_parquet('path/part-XXXX.parquet')` 구문으로 청크 분할 파일별 단일 적재 스트림을 정확히 실행하여 중복 적재를 방지합니다.
- **Resilient Fallback**: 서버 환경 제한으로 DuckDB 로드 실패 시, PostgreSQL `copy_expert` (In-memory CSV Copy 스트림) 및 MySQL/Oracle의 `executemany` (Array Binding 고속 삽입) 엔진으로 자동 전환되어 100% 실행 성공을 보장합니다.

### 3. Physical Key-Range Splitting & Keyset Pagination

- **Lock-Free 스캔**: 세션 연결 시 자동으로 `REPEATABLE READ` 또는 `READ COMMITTED` isolation 설정을 트리거하여 라이브 소스 DB 테이블에 락(Lock)을 전혀 유발하지 않습니다.
- **O(1) 수치형 PK 범위 쿼리**: 수치형 단일 PK 컬럼의 경우, PK의 Min/Max 범위를 균등 분할하여 `WHERE pk >= X AND pk < Y` 인덱스 범위 검색 방식으로 처리를 안전하게 진행합니다. (성능 저하를 일으키는 SQL `OFFSET` 방식 철저 금지)
- **커서 기반 키셋 페이지네이션 (비수치형 및 복합 PK)**: 단일 PK가 비수치형(String, DateTime 등)이거나 복합 PK(Composite PK)인 경우, 데이터를 정렬한 후 이전 청크의 마지막 레코드 ID(또는 복합 PK 값의 튜플)를 조건으로 사용해 `WHERE pk_col > last_pk` 방식으로 청크를 안전하게 쪼개고 슬라이싱(Keyset Pagination)합니다. 전체 PK 목록을 메모리에 로드하지 않아 메모리 부하(OOM)가 원천 방지됩니다.
- **사전 카운트 쿼리(`COUNT(*)`) 실행 목적**:
  - **청크 분할 수립 및 최적화**: 추출 전 테이블의 전체 행 수(`total_rows`)를 파악하여 `chunk_size` 대비 총 청크 수(`total_chunks`)를 미리 산정합니다. 데이터가 없는 빈 테이블은 즉시 추출을 스킵(Bypass)합니다.
  - **정확한 진척도 UI 렌더링**: 전체 진행률(Overall Progress) 및 테이블별 진행 상태바를 CLI 화면에 정확한 퍼센트(%) 수치로 표현하기 위한 기준값으로 활용됩니다.

### 4. 시각화 스키마 DDL 적용 및 로그 컨트롤 (Unlogged/Nologging)

- **트리 구조 스키마 DDL 적용**: Phase 2의 `prepare_schema` 단계에서 대상 테이블, 컬럼 수, UNLOGGED 설정 여부, 재실행 시 기존 데이터 초기화(TRUNCATE CASCADE / DELETE) 상태를 리치 트리(Tree) UI 형태로 시각화하여 한눈에 파악할 수 있습니다.
- **DDL 디커플링**: 스키마 생성 시 인덱스와 FK 생성을 뒤로 연기하고, 뼈대(Skeletal) 테이블 and PK만 먼저 생성하여 원본 데이터 삽입을 최고 속도로 진행합니다.
- **트랜잭션 로그 최적화**: 적재 시 대상 DB 트랜잭션 로그(WAL/Redo/Undo) 폭주를 막기 위해 PostgreSQL `UNLOGGED TABLE`, Oracle `NOLOGGING` 옵션을 적용합니다.
- **자동 시퀀스/PK 최적화**: 적재 완료 후 인덱스/FK를 일괄 복원하며, PostgreSQL 시퀀스, MySQL `AUTO_INCREMENT`, Oracle Identity 최댓값을 최종 적재된 데이터 값에 맞춰 자동 보정합니다. (복합 PK 테이블 시퀀스 보정 시 수치형 컬럼만 스마트하게 선별하여 처리)

### 5. 100% 오프라인 정합성 검증 (Offline Integrity Verification)

- **통합 무결성 검증 엔진 (`validate_all_tables`)**: 소스 DB와 연결을 해제한 상태에서, `DBValidator`가 전체 테이블의 청크 무결성 및 인덱스 생성을 일괄 검증합니다.
- **PK 기반 체크섬 검증 알고리즘**: 데이터 적재 후 Target DB 측과 Source DB(Parquet 추출 시점) 간의 PK 체크섬을 대조합니다.
  - **단일 수치형 PK(Integer 등)**: PK 값을 정수형(`BIGINT`)으로 변환한 뒤 **단순 합산(`SUM`)**하여 검증합니다.
    - SQL (Target DB): `SUM(CAST(pk_col AS BIGINT))`
    - Python (Source DB): `df.select(pl.col(pk_col).cast(pl.Int64).sum())`
  - **비수치형 PK 및 복합 PK(Composite PK)**: PK 컬럼들을 문자열로 연결(`CONCAT` 또는 `||`)한 뒤, 해시 함수를 통해 변환한 해시값들의 총합을 체크섬으로 사용합니다.
    - Python (Source DB): `df.select(concat_expr.hash(seed=0).sum())`
    - DuckDB (고속 덤프): `SUM(crc32(concat_expr))`
- **SQLAlchemy Inspector 연동**: Target DB에 실제로 생성된 인덱스 목록을 `inspect(engine)`을 통해 동적으로 수집하고 Source 메타데이터 인덱스 수와 일치하는지 자동으로 교차 검증합니다.
- ** 차이점 핀포인트 추적**: 불일치 청크 발견 시 로컬 Parquet 파일 데이터와 대상 DB 데이터를 로컬 메모리에 올려 정밀 **Polars Diff** 검증을 수행하고, 최대 10건의 핀포인트 mismatch 상세 내역을 `mismatch_log.json`에 기록합니다. 복합 PK 테이블도 **Polars의 다중 컬럼 Join 및 Expression 매칭**을 통해 메모리 효율적이고 정확하게 차이점을 추적하고 기록합니다.

### 6. 청크 단위 진척도 & 소요시간(Elapsed) 통합 시각화

- **청크 기반 진척율 보정**: Progress Bar의 진행 기준을 바이트 크기가 아닌 **처리된 청크 개수** 단위로 정렬하여 진척 상황에 부합하는 퍼센트(%) 진행률이 실시간 반영되도록 보정했습니다.
- **고도화된 Rich UI 구성**: 진행 정보가 화면을 어지럽히지 않도록 개별 텍스트 로그를 Progress Bar에 완전 통합하고, 실시간 진행 상태(예: `(용량계산중...)`, `(추출중...)`, `(적재중...)`, `(추출완료 - 12.34초)`) 및 커스텀 컬럼을 통해 처리 청크 수(`Extracted chunk 82/500`)와 현재 누적 저장 용량(`92.96 MB`)을 한 줄에 통합 제공합니다.
- **정밀 구간별 소요시간(Elapsed Time) 출력**: 메타데이터 추출, 사전 조사, 병렬 추출, 스키마 적용, 벌크 적재, 인덱스 복원, 오프라인 무결성 검증, 임시 파일 클린업 등 모든 마이그레이션 세션에 대한 구간 소요시간을 정밀하게 측정하여 요약 리포트합니다.

---

## 📦 설치 방법

1. 소스 코드를 마이그레이션 실행 폴더에 내려받습니다.
2. 필수 파이썬 의존 라이브러리를 설치합니다.

```bash
pip install -r requirements.txt

```

---

## 🛠️ CLI 옵션 상세

```bash
python main.py --help

```

- **`--mode`**: 실행 단계를 지정합니다.
- `extract`: Phase 1 모드로, 소스 데이터베이스에서 DDL 메타데이터와 대용량 데이터를 Parquet 포맷으로 디스크에 안전하게 덤프합니다.
- `load`: Phase 2 모드로, 대상 데이터베이스에 DDL 스키마를 번역하여 생성하고 데이터를 벌크 로드한 후 최종 데이터의 무결성을 검증합니다.
- **`--source-uri`**: 소스 데이터베이스의 접속 연결 URI입니다. (extract 모드 시 필수)
- 지원 포맷 예시:
- MySQL: `mysql+pymysql://유저:비밀번호@호스트:포트/DB명`
- PostgreSQL: `postgresql+psycopg2://유저:비밀번호@호스트:포트/DB명`
- Oracle: `oracle+oracledb://유저:비밀번호@호스트:포트/서비스명`
- **`--target-uri`**: 대상 데이터베이스의 접속 연결 URI입니다. (load 모드 시 필수)
- 접속 포맷은 `--source-uri`와 동일하며, 이기종 DB 간 교차 연결이 완벽 지원됩니다.
- **`--output-dir`**: 마이그레이션 메타데이터, 상태 체크포인트, 덤프된 Parquet 파티션 등이 모여 저장되는 단일 결과 폴더의 경로입니다. (기본값: `./migration_data`)
- 망분리 환경에서는 Phase 1에서 생성된 이 결과 폴더 자체를 물리 매체 또는 파일 전송 시스템을 통해 Phase 2 구동 서버로 복사하여 적재를 진행합니다.
- **`--chunk-size`**: PK 물리 범위 분할(Key-Range Splitting) 및 벌크 삽입 처리 시 사용하는 단일 청크(Chunk)당 레코드 행 수입니다. (기본값: `100,000`)
- 메모리(OOM) 한계와 인덱스 스캔 속도를 고려하여 10만~50만 범위를 권장합니다. 테이블 크기가 매우 클 때 이 크기를 단위로 Parquet 파일들이 나누어 생성됩니다.
- **`--delay-ms`**: 연속적으로 청크 데이터를 추출할 때, 소스 DB의 디스크 IOPS 폭주 및 버퍼 풀 부하를 조절하기 위해 청크 사이에 할당하는 미세 대기 시간(밀리초 단위)입니다. (기본값: `10` ms)
- 서비스 중인 소스 DB 마이그레이션 시 값을 늘려(예: 50~100ms) DB 서버의 부하를 효과적으로 억제할 수 있습니다.
- **`--max-workers`**: 여러 테이블을 동시에 추출하거나 로드할 때 사용할 최대 병렬 워커(스레드) 개수입니다. (기본값: `로컬 CPU 코어 수 - 1`)
- 값이 클수록 동시에 많은 테이블이 마이그레이션되나, 타겟 DB 서버의 커넥션 수(Connection Pool) 제한 및 로컬 CLI 장비의 RAM 점유율을 고려하여 적절히 제한할 수 있습니다.
- **`--resume`**: 마이그레이션 실패 시 이어받기(Resumability) 기능을 활성화합니다.
- 이 옵션을 활성화하면 작업 도중 네트워크 끊김, DB 세션 타임아웃 등으로 비정상 종료되더라도, 완료 상태 파일(`migration_progress*.json`)을 참조해 작업이 정상 완료된 테이블 및 파티션 청크 파일들을 자동으로 스킵하고 정확히 멈춘 파티션부터 마이그레이션을 재개하며, 기존 테이블의 TRUNCATE 처리를 건너뜁니다.
- **`--cleanup`**: 마이그레이션 및 오프라인 무결성 검증(`load` 단계)이 모두 완벽히 끝난 후, 디스크에 임시 저장되어 있던 대용량 Parquet 파일 폴더(`parquet/`)를 자동으로 제거하여 타겟 장비의 디스크 잔여 공간을 청소합니다.

---

## 💡 실행 예시

### Phase 1: Extract (소스 DB에서 고속 Parquet 덤프)

소스 DB에만 연결하여 스키마 정의 파일 및 분할 Parquet 파일을 산출합니다:

```bash
python main.py --mode extract --source-uri "postgresql://postgres:root@127.0.0.1:5432/my-app-db" --output-dir "./mig_assets" --chunk-size 100000 --delay-ms 10 --max-workers 3

```

### Phase 2: Load & Validate (대상 DB에 벌크 적재 및 검증)

이동 매체 또는 타겟 서버 환경에서 대상 DB에만 연결하여 DDL 트리 시각화 생성, 데이터 단일 파일 단위 벌크 로드, 종합 검증(`validate_all_tables`) 및 시퀀스 최적화를 수행합니다:

```bash
python main.py --mode load --target-uri "postgresql://postgres:root@127.0.0.1:5432/my-app-db" --output-dir "./mig_assets" --max-workers 3 --cleanup

```

#### 🖥️ Phase 1 추출 진행 상황 콘솔 출력 예시

```text
Extracting database metadata and structures...
🔍 DDL Metadata | Table: batch_checkpoints             (5 cols, PK: id)
🔍 DDL Metadata | Table: bulk_test_users               (8 cols, PK: id)
✓ Database metadata extracted successfully (Elapsed: 0.85s)

Discovered 2 tables to extract.
Pre-scanning tables to estimate total rows...
  📊 Table: batch_checkpoints             | Rows: 4
  📊 Table: bulk_test_users               | Rows: 50,000,000
Total estimated rows to extract: 50,000,004 (Pre-scan took 1.12s)

Table: batch_checkpoints (추출완료 - 0.05초) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% Extracted chunk 1/1 0.00 MB
Table: bulk_test_users (추출중...) ━━━━━━━━━━╸━━━━━━━━━━━━━━━━━━━  16% Extracted chunk 82/500 92.96 MB
[bold yellow]Overall Progress[/bold yellow] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100%
✓ Extraction phase completed! Parquet files and metadata stored successfully. (Elapsed: 45.23s)
```

#### 🖥️ Phase 2 스키마 생성 및 적재 진행 상황 콘솔 출력 예시

```text
Step 1/4: Applying skeletal schema DDL...

 📦 Table: batch_checkpoints
   └── 🏗️ Created Table (UNLOGGED) (5 columns)

 📦 Table: bulk_test_users
   └── 🏗️ Created Table (UNLOGGED) (8 columns)

✓ Skeletal tables created. (Elapsed: 2.14s)

Step 2/4: Bulk loading data chunks into Target DB...

Total data volume to load: 594.30 MB
  📦 Table: batch_checkpoints             | Size: 0.00 MB
  📦 Table: bulk_test_users               | Size: 594.30 MB

Table: batch_checkpoints (적재완료 - 0.02초) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100% Loaded chunk 1/1 0.00 MB
Table: bulk_test_users (적재중...) ━━━━━━━━━━━━━━━━━━━╸━━━━━━━━━━━  50% Loaded chunk 250/500 297.15 MB
[bold yellow]Overall Progress[/bold yellow] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100%
✓ Bulk loading completed. (Elapsed: 56.78s)

---

## 🔄 핵심 파이프라인 루프 및 처리 구조

VeloxDB의 데이터 추출, 적재, 검증의 전체 단계는 청크(Chunk) 및 테이블 단위의 세밀한 루프 제어를 통해 안정적으로 진행됩니다.

### 1. 데이터 추출 (Phase 1: Extract)
* **테이블 내부 청크 단위 루프**: `DBExtractor.extract_table`
  * 하나의 테이블 데이터를 나눈 청크 범위 목록(`ranges`)을 기준으로 루프를 돌며 데이터를 파일로 덤프하고 체크섬을 연산합니다.
  * 루프 문: `for idx, range_info in enumerate(ranges):`
* **전체 테이블 루프 (병렬)**: `main.py -> run_parallel_extraction`
  * 스레드 풀(`ThreadPoolExecutor`)을 사용해 여러 테이블의 `extract_table` 작업을 병렬로 호출합니다.

### 2. 데이터 적재 (Phase 2: Load)
* **테이블 내부 청크 파일 루프**: `DBLoader.load_table_data`
  * 저장된 Parquet 파티션 파일 목록(`part-*.parquet`)을 차례대로 읽는 루프를 돌면서 타겟 DB에 데이터 벌크 적재(`_bulk_insert_df`)를 실행합니다.
  * 루프 문: `for idx, p_file in enumerate(parquet_files):`
* **전체 테이블 루프 (병렬)**: `main.py -> run_parallel_load`
  * 대상 테이블 목록을 대상으로 병렬 적재 세션을 관리합니다.

### 3. 적재 완료 후 검증 (Phase 2: Validate)
* **테이블 내부 청크 체크섬 검증 루프**: `DBValidator.validate_table`
  * `source_checksums.json`에서 읽어온 소스 DB의 청크별 체크섬 정보를 기준으로 루프를 수행하며 타겟 DB의 집계 결과와 대조합니다.
  * 루프 문: `for chunk in chunks:`
* **전체 테이블 검증 루프**: `DBValidator.validate_all_tables`
  * 메타데이터 내 테이블 목록을 순회하며 테이블마다 검증 함수를 실행시킵니다.
  * 루프 문: `for table in tables:`

---

## 📂 결과물 폴더 구조

마이그레이션 출력 디렉터리(`--output-dir`) 내부에 아래와 같은 독립 자산들이 통합 관리됩니다.

```
migration_data/
├── schema.sql                   # 원본 소스 DB 테이블 DDL 정보
├── metadata.json                # 컬럼 목록, 인덱스 정보, FK 관계가 담긴 번역용 JSON 메타
├── source_checksums.json        # 청크 단위 PK 범위 및 MD5 해시 통계 (오프라인 검증용)
├── migration_progress.json      # Phase 1 추출 체크포인트
├── migration_progress_load.json # Phase 2 적재 체크포인트
├── mismatch_log.json            # 정합성 오류 발생 시 차이 데이터 샘플 로그 (mismatch 존재 시 자동 생성)
├── migration_report.md          # 마이그레이션 최종 검증 종합 리포트
└── parquet/                     # 물리적 분할 저장된 Parquet 파티션 폴더
    ├── users/
    │   ├── part-0000.parquet
    │   └── part-0001.parquet
    └── orders/
        ├── part-0000.parquet
        └── part-0001.parquet

```

---

## 🔄 망분리 환경에서의 오프라인 Dialect 변환 매커니즘

VeloxDB는 소스(Source) DB와 대상(Target) DB가 동시에 연결될 필요가 없는 철저한 **망분리 친화적 구조**를 가집니다. 변환 및 적재는 다음과 같이 안전하게 처리됩니다.

1. **스키마 정보의 추상화 (Phase 1)**:

- 소스 DB 접속 상태에서 `inspect` 모듈을 통해 원본 테이블들의 메타데이터(컬럼명, 물리 데이터 타입, PK/FK 구조)를 추출합니다.
- 추출한 정보와 소스 DB의 종류(예: `mysql`, `postgresql`, `oracle`)를 `metadata.json` 파일의 `__source_dialect__` 헤더에 기록합니다.

2. **실시간 Dialect 번역 및 실행 (Phase 2)**:

- 대상 DB 접속 상태에서 `metadata.json` 파일만 로드합니다.
- `DialectConverter` 클래스가 `__source_dialect__` 정보와 대상 DB의 커넥션 정보를 바탕으로, 대상 DB 스펙에 맞게 데이터 타입을 동적으로 변환(예: `NUMBER` ➡ `BIGINT`, `VARCHAR2` ➡ `VARCHAR`)합니다.
- 번역 완료된 DDL 스키마가 대상 DB에 실행되므로 소스 DB에 다시 접속하지 않고도 스키마 변환 및 적재가 이루어집니다.

---

## 📊 리포트 및 검증 프로세스

1. **[Source DB] 추출 시점 (Phase 1)**

- 테이블 구조, Total Rows, Index 목록 및 컬럼 메타데이터를 `metadata.json`에 기록.

2. **[Target DB] 로드 및 검증 시점 (`DBValidator`)**

- Target DB에 직접 연결하여 테이블별 `SELECT COUNT(*)` 및 SQLAlchemy Inspector 기반 타겟 인덱스 조회.
- `metadata.json` 및 `source_checksums.json`의 원본 통계 수치와 청크 범위/인덱스 수 교차 비교.
- 비교 결과(`validation_results`)를 콘솔에 테이블 형태로 시각화 출력 (테이블별 Src/Tgt Row Count, Src/Tgt Index Count 일치 여부 포함).

3. **[Markdown 리포트 생성] (`ReportGenerator`)**

- 검증 결과를 바탕으로 `migration_report.md` 문서를 최종 자동 생성.

---

## 🛠️ PostgreSQL 검증 및 시퀀스 보정 SQL 예시

### 테이블 용량 및 인덱스 세부 조회 쿼리

```sql
SELECT
    relname AS table_name,
    pg_size_pretty(pg_relation_size(c.oid)) AS table_size, -- 순수 테이블 데이터 용량
    pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid) - COALESCE(pg_relation_size(c.reltoastrelid), 0)) AS index_size, -- 인덱스 용량
    pg_size_pretty(COALESCE(pg_relation_size(c.reltoastrelid), 0)) AS toast_size, -- TOAST(대용량 컬럼) 용량
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size -- 전체 용량
FROM pg_class c
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND c.relkind = 'r'
  AND relname = '테이블명'; -- 확인하실 테이블명 입력

```

### 시퀀스(Auto Increment) 번호 맞추기 (신규 데이터 INSERT 시 중복 에러 방지)

```sql
SELECT setval(pg_get_serial_sequence('테이블명', 'PK컬럼명'), COALESCE(max("PK컬럼명"), 1)) FROM "테이블명";

```

### 통계 정보 최적화 (쿼리 조회 속도 향상)

```sql
ANALYZE VERBOSE "테이블명";
```
