import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import probe


class PublicReadBoundaryTests(unittest.TestCase):
    def setUp(self):
        probe.configure(max_requests=4, deadline_seconds=30)

    def test_redemption_auth_and_external_urls_never_reach_network(self):
        invalid = [
            probe.ORIGIN + '/campanhasdeingresso/pPS/resgatar',
            probe.ORIGIN + '/auth/uol/login',
            probe.ORIGIN + '/logout',
            probe.ORIGIN + '/campanhasdeingresso/pPS?next=resgatar',
            probe.ORIGIN + '/campanhasdeingresso/../logout',
            'https://evil.test/campanhasdeingresso/pPS',
        ]
        with patch('probe.urllib.request.build_opener') as opener:
            for url in invalid:
                with self.subTest(url=url), self.assertRaises(ValueError):
                    probe.get(url)
            opener.assert_not_called()
        self.assertEqual(probe.request_count(), 0)

    def test_redirect_to_redemption_is_not_followed(self):
        with patch('probe.get', return_value=(302, {'Location':'/campanhasdeingresso/pPS/resgatar'}, '', '')) as get:
            result = probe.probe_code('pPS')
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['reason'], 'unexpected_redirect')
        get.assert_called_once_with(probe.ORIGIN+'/campanhasdeingresso/pPS')

    def test_case_and_identity_are_preserved(self):
        self.assertTrue(probe.allowed('/campanhasdeingresso/pPS-show', 'pPS', True))
        self.assertEqual(probe.allowed('/campanhasdeingresso/pPs-show', 'pPS', True), '')
        self.assertEqual(probe.allowed('/campanhasdeingresso/p.PS-show', 'pPS', True), '')

    def test_missing_redirect_is_inconclusive(self):
        with patch('probe.get', return_value=(302, {}, '', '')):
            result = probe.probe_code('pPS')
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['reason'], 'missing_redirect_location')

    def test_page_presence_does_not_claim_stock(self):
        url=probe.ORIGIN+'/campanhasdeingresso/pPS-show'
        html=f'''<html><head><link rel="canonical" href="{url}">
        <meta property="og:url" content="{url}"></head><body>
        <div id="beneficio"><h2>2 INGRESSOS: 13/10 Nubank Parque SP</h2>
        <div class="info-beneficio"><p>Robbie Williams: show em São Paulo em 13 de outubro de 2026.</p></div></div>
        <p>Benefício válido de 08/10/2026 13:16 até 13/10/2026 13:00.</p></body></html>'''
        with patch('probe.get', return_value=(200, {'Content-Type':'text/html'}, html, '')):
            result=probe.detail(url,'pPS')
        self.assertEqual(result['status'],'found')
        self.assertEqual(result['stock'],'unknown')
        self.assertEqual(len(result['validity']),1)

    def test_budget_stops_before_another_request(self):
        probe.configure(max_requests=1,deadline_seconds=30)
        url=probe.ORIGIN+'/campanhasdeingresso/pPS'
        opener=Mock()
        opener.open.side_effect=HTTPError(url,302,'redirect',{'Location':'/'},None)
        with patch('probe.urllib.request.build_opener',return_value=opener), patch('probe.time.sleep'):
            self.assertEqual(probe.get(url)[0],302)
            self.assertEqual(probe.get(url)[3],'request_budget')
        self.assertEqual(opener.open.call_count,1)
        self.assertEqual(probe.request_count(),1)

    def test_rate_limit_stops_following_requests(self):
        url=probe.ORIGIN+'/campanhasdeingresso/pPS'
        opener=Mock()
        opener.open.side_effect=HTTPError(url,429,'rate limited',{},None)
        with patch('probe.urllib.request.build_opener',return_value=opener), patch('probe.time.sleep'):
            self.assertEqual(probe.get(url)[0],429)
            self.assertEqual(probe.get(url)[3],'http_429')
        self.assertEqual(opener.open.call_count,1)


if __name__=='__main__':
    unittest.main()
