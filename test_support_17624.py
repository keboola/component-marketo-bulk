"""Regression guard for SUPPORT-17624.

Marketo stopped accepting the OAuth token as an ``access_token`` query
parameter on 2026-08-31. Every Bulk Extract request must carry it in an
``Authorization: Bearer`` header instead.

All Bulk Extract calls go through Component.get_request and
Component.post_request, so these tests cover every call site.
"""
import os
import sys
import unittest
from unittest import mock

# component.py builds a GELF log handler when it is imported. The handler does
# not open a socket until the first log record, so these values are enough.
os.environ.setdefault('KBC_LOGGER_ADDR', 'localhost')
os.environ.setdefault('KBC_LOGGER_PORT', '12201')

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from component import Component  # noqa: E402


class FakeResponse(object):
    status_code = 200

    def json(self):
        return {'success': True}


def build_client():
    """A Component with a known token, without calling the auth endpoint."""
    client = Component()
    client.BASE_URL = 'https://munchkin.mktorest.com'
    client.access_token = 'TOKEN'
    client.desired_activities = []
    client.fields_str = 'id,email'
    return client


class TestTokenTransport(unittest.TestCase):
    """The token must travel in the header, never in the query string."""

    def assert_bearer(self, request):
        kwargs = request.call_args[1]
        self.assertEqual('Bearer TOKEN', kwargs['headers'].get('Authorization'))
        self.assertNotIn('access_token', kwargs.get('params') or {})

    def test_get_request_uses_authorization_header(self):
        client = build_client()
        with mock.patch('component.requests.get', return_value=FakeResponse()) as request:
            client.get_request('https://munchkin.mktorest.com/bulk/v1/leads/export.json',
                               params={})
        self.assert_bearer(request)

    def test_post_request_uses_authorization_header(self):
        client = build_client()
        with mock.patch('component.requests.post', return_value=FakeResponse()) as request:
            client.post_request('https://munchkin.mktorest.com/bulk/v1/leads/export/create.json',
                                params={}, body={'format': 'CSV'})
        self.assert_bearer(request)


class TestExportParamsCarryNoToken(unittest.TestCase):
    """The query params built for an export must not contain the token."""

    def test_fetch_endpoint_params_have_no_access_token(self):
        client = build_client()
        calls = []

        def record(url, request_param, *args, **kwargs):
            calls.append(request_param)
            raise StopIteration('stop after the first call')

        date_obj = {
            'updated_date_bool': True,
            'start_updated_date': '2026-09-01',
            'end_updated_date': '2026-09-02',
            'created_date_bool': False,
        }
        with mock.patch.object(Component, 'create_mkto_export', side_effect=record):
            try:
                client.fetch_endpoint('leads', date_obj)
            except StopIteration:
                pass

        self.assertEqual(1, len(calls))
        self.assertNotIn('access_token', calls[0])


if __name__ == '__main__':
    unittest.main()
