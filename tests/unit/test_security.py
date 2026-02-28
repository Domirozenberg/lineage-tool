"""Unit tests for app.core.security."""

import os
import tempfile
import time
from datetime import timedelta

import pytest
from jose import JWTError

from app.core.security import (
    API_KEY_PREFIX,
    create_access_token,
    create_refresh_token,
    decode_token,
    generate_api_key,
    hash_api_key,
    hash_password,
    validate_offline_folder,
    verify_api_key,
    verify_password,
)


class TestPasswordHashing:
    def test_hash_is_different_from_plain(self):
        plain = "supersecret"
        hashed = hash_password(plain)
        assert hashed != plain

    def test_verify_correct_password(self):
        plain = "supersecret"
        assert verify_password(plain, hash_password(plain))

    def test_verify_wrong_password(self):
        assert not verify_password("wrong", hash_password("right"))

    def test_same_plain_produces_different_hashes(self):
        h1 = hash_password("abc")
        h2 = hash_password("abc")
        assert h1 != h2  # bcrypt uses random salt


class TestJwt:
    def test_access_token_round_trip(self):
        token = create_access_token("user-123", "admin")
        payload = decode_token(token)
        assert payload["sub"] == "user-123"
        assert payload["role"] == "admin"
        assert payload["type"] == "access"

    def test_refresh_token_round_trip(self):
        token = create_refresh_token("user-456")
        payload = decode_token(token)
        assert payload["sub"] == "user-456"
        assert payload["type"] == "refresh"

    def test_access_token_type_differs_from_refresh(self):
        access = create_access_token("u", "user")
        refresh = create_refresh_token("u")
        assert decode_token(access)["type"] == "access"
        assert decode_token(refresh)["type"] == "refresh"

    def test_expired_token_raises(self):
        token = create_access_token("u", "user", expires_delta=timedelta(seconds=-1))
        with pytest.raises(JWTError):
            decode_token(token)

    def test_tampered_token_raises(self):
        token = create_access_token("u", "user")
        # Flip a character in the signature
        tampered = token[:-3] + "AAA"
        with pytest.raises(JWTError):
            decode_token(tampered)

    def test_refresh_token_not_usable_as_access(self):
        """Callers must check the 'type' field — this documents the contract."""
        refresh = create_refresh_token("u")
        payload = decode_token(refresh)
        assert payload["type"] != "access"


class TestApiKeys:
    def test_generated_key_has_prefix(self):
        key = generate_api_key()
        assert key.startswith(API_KEY_PREFIX)

    def test_generated_keys_are_unique(self):
        keys = {generate_api_key() for _ in range(100)}
        assert len(keys) == 100

    def test_hash_is_deterministic(self):
        key = generate_api_key()
        assert hash_api_key(key) == hash_api_key(key)

    def test_verify_correct_key(self):
        key = generate_api_key()
        stored = hash_api_key(key)
        assert verify_api_key(key, stored)

    def test_verify_wrong_key(self):
        key = generate_api_key()
        stored = hash_api_key(key)
        assert not verify_api_key("lng_wrongkey", stored)


class TestValidateOfflineFolder:
    def test_valid_folder_with_required_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            for fname in ("tables.json", "columns.json"):
                open(os.path.join(tmpdir, fname), "w").close()
            errors = validate_offline_folder(tmpdir, ["tables.json", "columns.json"])
            assert errors == []

    def test_folder_does_not_exist(self):
        errors = validate_offline_folder("/nonexistent/path", ["file.json"])
        assert len(errors) == 1
        assert "does not exist" in errors[0]

    def test_missing_required_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            open(os.path.join(tmpdir, "tables.json"), "w").close()
            errors = validate_offline_folder(tmpdir, ["tables.json", "columns.json"])
            assert len(errors) == 1
            assert "columns.json" in errors[0]

    def test_empty_required_files_is_valid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            errors = validate_offline_folder(tmpdir, [])
            assert errors == []

    def test_all_files_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            errors = validate_offline_folder(tmpdir, ["a.json", "b.json", "c.json"])
            assert len(errors) == 3
