from src.auth import BaseMinioAuth

def test_connection():
    minio = BaseMinioAuth()
    minio_client = minio.get_client()
    try:
        minio_client.list_buckets()
        return print("Minio client connected successfully")
    except Exception as e:
        raise ConnectionError(f"Failed to connect to Minio: {e}")
    