"""
Copyright (C) 2026, Pelican Project, Morgridge Institute for Research

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
import pytest
from pytest_httpserver import HTTPServer

from pelicanfs.core import PelicanFileSystem, _CacheManager


def test_cat_file_404_raises_file_not_found(httpserver: HTTPServer, get_client):
    """Two sequential 404s must both raise FileNotFoundError, not crash with ValueError."""
    foo_bar_url = httpserver.url_for("/foo/bar")
    httpserver.expect_request("/.well-known/pelican-configuration").respond_with_json({"director_endpoint": httpserver.url_for("/")})
    # Director redirect — only needed on the first call; namespace is cached after that
    httpserver.expect_oneshot_request("/foo/bar", method="GET").respond_with_data(
        "",
        status=307,
        headers={
            "Link": f'<{foo_bar_url}>; rel="duplicate"; pri=1; depth=1',
            "Location": foo_bar_url,
            "X-Pelican-Namespace": "namespace=/foo",
        },
    )
    # First 404
    httpserver.expect_oneshot_request("/foo/bar", method="HEAD").respond_with_data("", status=404)
    httpserver.expect_oneshot_request("/foo/bar", method="GET").respond_with_data("", status=404)
    # Second 404 — this is what triggered ValueError before the fix
    httpserver.expect_oneshot_request("/foo/bar", method="HEAD").respond_with_data("", status=404)
    httpserver.expect_oneshot_request("/foo/bar", method="GET").respond_with_data("", status=404)

    pelfs = PelicanFileSystem(
        httpserver.url_for("/"),
        get_client=get_client,
        skip_instance_cache=True,
    )

    with pytest.raises(FileNotFoundError):
        pelfs.cat("/foo/bar")
    with pytest.raises(FileNotFoundError):
        pelfs.cat("/foo/bar")


def test_404_does_not_evict_cache(httpserver: HTTPServer, get_client):
    """A 404 must not evict the healthy cache from the namespace cache."""
    foo_bar_url = httpserver.url_for("/foo/bar")
    httpserver.expect_request("/.well-known/pelican-configuration").respond_with_json({"director_endpoint": httpserver.url_for("/")})
    httpserver.expect_oneshot_request("/foo/bar", method="GET").respond_with_data(
        "",
        status=307,
        headers={
            "Link": f'<{foo_bar_url}>; rel="duplicate"; pri=1; depth=1',
            "Location": foo_bar_url,
            "X-Pelican-Namespace": "namespace=/foo",
        },
    )
    httpserver.expect_oneshot_request("/foo/bar", method="HEAD").respond_with_data("", status=404)
    httpserver.expect_oneshot_request("/foo/bar", method="GET").respond_with_data("", status=404)

    pelfs = PelicanFileSystem(
        httpserver.url_for("/"),
        get_client=get_client,
        skip_instance_cache=True,
    )

    with pytest.raises(FileNotFoundError):
        pelfs.cat("/foo/bar")

    namespace_info = pelfs._get_prefix_info("/foo/bar")
    assert namespace_info is not None
    assert len(namespace_info.cache_manager._cache_list) > 0


def test_bad_cache_idempotent():
    """_CacheManager.bad_cache() called twice for the same URL must not raise."""
    cm = _CacheManager(["https://cache.example.com/"])
    cm.bad_cache("https://cache.example.com/")
    cm.bad_cache("https://cache.example.com/")  # must not raise ValueError
