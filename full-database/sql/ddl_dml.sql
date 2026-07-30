-- public.batch_checkpoints definition

-- Drop table

-- DROP TABLE public.batch_checkpoints;

CREATE TABLE public.batch_checkpoints (
	job_name varchar(100) NOT NULL,
	last_processed_id int8 NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	CONSTRAINT batch_checkpoints_job_name_not_null NOT NULL job_name,
	CONSTRAINT batch_checkpoints_last_processed_id_not_null NOT NULL last_processed_id,
	CONSTRAINT batch_checkpoints_pkey PRIMARY KEY (job_name),
	CONSTRAINT batch_checkpoints_updated_at_not_null NOT NULL updated_at
);


-- public.bulk_test_users definition

-- Drop table

-- DROP TABLE public.bulk_test_users;

CREATE TABLE public.bulk_test_users (
	id serial4 NOT NULL,
	user_id varchar(50) NOT NULL,
	username varchar(100) NOT NULL,
	email varchar(150) NULL,
	score int4 NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	updated_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT bulk_test_users_created_at_not_null NOT NULL created_at,
	CONSTRAINT bulk_test_users_id_not_null NOT NULL id,
	CONSTRAINT bulk_test_users_pkey PRIMARY KEY (id),
	CONSTRAINT bulk_test_users_updated_at_not_null NOT NULL updated_at,
	CONSTRAINT bulk_test_users_user_id_not_null NOT NULL user_id,
	CONSTRAINT bulk_test_users_username_not_null NOT NULL username
);
CREATE UNIQUE INDEX ix_bulk_test_users_user_id ON public.bulk_test_users USING btree (user_id);

-- 1. 기존 잔여 데이터 및 충돌 방지를 위해 테이블 초기화
TRUNCATE TABLE public.bulk_test_users;

-- 2. 자릿수를 7자리 -> 9자리로 늘려 오버플로우 완벽 방지 후 실행
INSERT INTO bulk_test_users (
    user_id, username, email, score, created_at, updated_at
)
SELECT
    -- 💡 lpad 자릿수를 9로 늘려서 11,000,000 문자열이 안전하게 포함되도록 함
    'USER_' || lpad((s.id + 10_000_000)::text, 9, '0') AS user_id,

    (ARRAY['위대한', '슬픈', '짜릿한', '비밀스러운', '마지막', '화려한', '어두운', '지독한', '달콤한', '위험한'])[floor(random() * 10 + 1)] || ' ' ||
    (ARRAY['괴물', '기생충', '올드보이', '부산행', '명량', '극한직업', '범죄도시', '신과함께', '인터스텔라', '아바타'])[floor(random() * 10 + 1)] AS username,

    'test_user_' || (s.id + 10_000_000) || '@example.com' AS email,
    floor(random() * 101)::int AS score,
    NOW() - (random() * 30 * INTERVAL '1 day') AS created_at,
    NOW() AS updated_at
FROM generate_series(1, 50_000_000) AS s(id);

analyze bulk_test_users;
