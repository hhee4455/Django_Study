import subprocess
import time

# PostgreSQL 서버가 사용 가능할 때까지 대기하는 함수
def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    """
    :param host: PostgreSQL 서버 호스트 주소
    :param max_retries: 최대 재시도 횟수 (기본값: 5)
    :param delay_seconds: 재시도 간격 (초) (기본값: 5)
    :return: PostgreSQL 서버 연결 성공 여부 (True or False)
    """
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True)
            if "accepting connections" in result.stdout:
                print("Successfully connected to PostgreSQL!")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            retries += 1
            print(
                f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting.")
    return False

# ELT 스크립트 실행 전에 PostgreSQL 서버에 연결을 시도
if not wait_for_postgres(host="source_postgres"):
    exit(1)

print("Starting ELT script...")

# 소스 PostgreSQL 데이터베이스 구성
source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'source_postgres'  # 도커 컴포즈의 서비스 이름을 호스트로 사용
}

# 대상 PostgreSQL 데이터베이스 구성
destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    'host': 'destination_postgres'  # 도커 컴포즈의 서비스 이름을 호스트로 사용
}

# pg_dump를 사용하여 소스 데이터베이스를 SQL 파일로 덤프
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'  # 암호를 묻지 않음
]

# 암호를 묻지 않도록 하기 위해 PGPASSWORD 환경 변수 설정
subprocess_env = dict(PGPASSWORD=source_config['password'])

# 덤프 명령 실행
subprocess.run(dump_command, env=subprocess_env, check=True)

# psql을 사용하여 덤프된 SQL 파일을 대상 데이터베이스에 로드
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

# 대상 데이터베이스에 대한 PGPASSWORD 환경 변수 설정
subprocess_env = dict(PGPASSWORD=destination_config['password'])

# 로드 명령 실행
subprocess.run(load_command, env=subprocess_env, check=True)

print("Ending ELT script...")
