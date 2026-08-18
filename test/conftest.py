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

import os
import ssl
from contextlib import asynccontextmanager

import aiohttp
import pytest
import trustme
from pytest_httpserver import HTTPServer

import pelicanfs.core


@pytest.fixture(scope="session", name="ca")
def fixture_ca():
    return trustme.CA()


@pytest.fixture(scope="session", name="httpserver_listen_address")
def fixture_httpserver_listen_address():
    return ("localhost", 0)


@pytest.fixture(scope="session", name="httpserver_ssl_context")
def fixture_httpserver_ssl_context(ca):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    localhost_cert = ca.issue_cert("localhost")
    localhost_cert.configure_cert(context)
    return context


@pytest.fixture(scope="session", name="httpclient_ssl_context")
def fixture_httpclient_ssl_context(ca):
    with ca.cert_pem.tempfile() as ca_temp_path:
        return ssl.create_default_context(cafile=ca_temp_path)


@pytest.fixture(scope="session", name="httpserver2")
def fixture_httpserver2(httpserver_listen_address, httpserver_ssl_context):
    host, port = httpserver_listen_address
    if not host:
        host = HTTPServer.DEFAULT_LISTEN_HOST
    if not port:
        port = HTTPServer.DEFAULT_LISTEN_PORT

    server = HTTPServer(host=host, port=port, ssl_context=httpserver_ssl_context)
    server.start()
    yield server
    server.clear()
    if server.is_running():
        server.stop()


@pytest.fixture(scope="session", name="threaded_httpserver")
def fixture_threaded_httpserver(httpserver_listen_address, httpserver_ssl_context):
    """
    A server that serves connections in parallel rather than one at a time.

    The default server is single threaded, so a connection left open blocks the next
    request from being served at all. A test that checks connections are not being held
    open needs to make a second request while the first would still be held, and against
    the default server that request waits for aiohttp's connect timeout and fails on
    that instead of on the thing it set out to assert.
    """
    host, port = httpserver_listen_address
    if not host:
        host = HTTPServer.DEFAULT_LISTEN_HOST
    if not port:
        port = HTTPServer.DEFAULT_LISTEN_PORT

    server = HTTPServer(host=host, port=port, ssl_context=httpserver_ssl_context, threaded=True)
    server.start()
    yield server
    server.clear()
    if server.is_running():
        server.stop()


@pytest.fixture(scope="session", name="get_client")
def fixture_get_client(httpclient_ssl_context):
    async def client_factory(**kwargs):
        connector = aiohttp.TCPConnector(ssl=httpclient_ssl_context)
        return aiohttp.ClientSession(connector=connector, **kwargs)

    return client_factory


@pytest.fixture(scope="session", name="get_webdav_client")
def fixture_get_webdav_client(httpclient_ssl_context):
    """
    Hand PelicanFileSystem the production get_webdav_client, pointed at the test CA.

    This deliberately delegates rather than building a client of its own, so that the
    tests exercise the real client setup - including the release of the responses it
    produces - instead of a copy of it that can drift.
    """

    @asynccontextmanager
    async def client_factory(options, **kwargs):
        async with pelicanfs.core.get_webdav_client({**options, "verify_ssl": httpclient_ssl_context}) as client:
            yield client

    return client_factory


@pytest.fixture
def top_listing_response():
    file_path = os.path.join(os.path.dirname(__file__), "resources", "top_xml_response.xml")
    with open(file_path, "r") as f:
        return f.read()


@pytest.fixture
def f1_listing_response():
    file_path = os.path.join(os.path.dirname(__file__), "resources", "f1_xml_response.xml")
    with open(file_path, "r") as f:
        return f.read()


@pytest.fixture
def f2_listing_response():
    file_path = os.path.join(os.path.dirname(__file__), "resources", "f2_xml_response.xml")
    with open(file_path, "r") as f:
        return f.read()


@pytest.fixture
def sf_listing_response():
    file_path = os.path.join(os.path.dirname(__file__), "resources", "sf_xml_response.xml")
    with open(file_path, "r") as f:
        return f.read()


@pytest.fixture
def file1_listing_response():
    file_path = os.path.join(os.path.dirname(__file__), "resources", "file1_xml_response.xml")
    with open(file_path, "r") as f:
        return f.read()


@pytest.fixture
def file2_listing_response():
    file_path = os.path.join(os.path.dirname(__file__), "resources", "file2_xml_response.xml")
    with open(file_path, "r") as f:
        return f.read()


@pytest.fixture
def file3_listing_response():
    file_path = os.path.join(os.path.dirname(__file__), "resources", "file3_xml_response.xml")
    with open(file_path, "r") as f:
        return f.read()


@pytest.fixture
def f1_file1_listing_response():
    file_path = os.path.join(os.path.dirname(__file__), "resources", "f1_file1_xml_response.xml")
    with open(file_path, "r") as f:
        return f.read()


@pytest.fixture
def sf_file_listing_response():
    file_path = os.path.join(os.path.dirname(__file__), "resources", "sf_file_xml_response.xml")
    with open(file_path, "r") as f:
        return f.read()


@pytest.fixture
def f2_file1_listing_response():
    file_path = os.path.join(os.path.dirname(__file__), "resources", "f2_file1_xml_response.xml")
    with open(file_path, "r") as f:
        return f.read()


@pytest.fixture
def f2_file2_listing_response():
    file_path = os.path.join(os.path.dirname(__file__), "resources", "f2_file2_xml_response.xml")
    with open(file_path, "r") as f:
        return f.read()
