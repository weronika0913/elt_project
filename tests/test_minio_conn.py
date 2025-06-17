"""
Integration tests for the auth module.
"""

from src.auth import BaseMinioAuth


def test_connection():
    """
    Tests connection to the Minio client and listing of buckets.
    """
    minio = BaseMinioAuth()
    minio_client = minio.get_client()
    buckets = minio_client.list_buckets()
    assert isinstance(buckets, list)
