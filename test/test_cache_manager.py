"""
Copyright (C) 2025, Pelican Project, Morgridge Institute for Research

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License.  You may
obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import time

import pytest

from pelicanfs.core import _CacheManager
from pelicanfs.exceptions import NoAvailableSource


def test_cache_manager_ttl_recovery():
    """Test that bad caches recover after TTL expires."""
    cm = _CacheManager(["https://cache1.example.com"], bad_cache_ttl=1)
    cm.bad_cache("https://cache1.example.com/file")

    # Cache is bad, should raise
    with pytest.raises(NoAvailableSource):
        cm.get_url("/file")

    # Wait for TTL to expire
    time.sleep(1.5)

    # Cache should be recovered
    url = cm.get_url("/file")
    assert "cache1" in url


def test_cache_manager_bad_cache_not_recovered_before_ttl():
    """Test that bad caches stay bad until TTL expires."""
    cm = _CacheManager(["https://cache1.example.com"], bad_cache_ttl=60)
    cm.bad_cache("https://cache1.example.com/file")

    # Cache should still be bad (TTL not expired)
    with pytest.raises(NoAvailableSource):
        cm.get_url("/file")


def test_cache_manager_multiple_caches_fallback():
    """Test that when one cache is bad, the other is used."""
    cm = _CacheManager(["https://cache1.example.com", "https://cache2.example.com"], bad_cache_ttl=60)

    # Mark first cache as bad
    cm.bad_cache("https://cache1.example.com/file")

    # Should get URL from second cache
    url = cm.get_url("/file")
    assert "cache2" in url


def test_cache_manager_bad_cache_recovery_order():
    """Test that recovered caches are appended to the end of the list."""
    cm = _CacheManager(["https://cache1.example.com", "https://cache2.example.com"], bad_cache_ttl=1)

    # Mark first cache as bad
    cm.bad_cache("https://cache1.example.com/file")

    # Second cache should be preferred now
    url = cm.get_url("/file")
    assert "cache2" in url

    # Wait for TTL
    time.sleep(1.5)

    # cache2 should still be first (cache1 recovered but appended to end)
    url = cm.get_url("/file")
    assert "cache2" in url
