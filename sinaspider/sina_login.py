"""
A weibo login module.
"""

import base64
import binascii
import json
import logging
import re
import requests
import rsa

logger = logging.getLogger(__name__)


class ResponseUtils(object):
    @staticmethod
    def parseJson(text):
        return ResponseUtils.match('\(({.*})\)', text)

    @staticmethod
    def parseRedirectUrl(text):
        return ResponseUtils.match(
            'location\.replace\([\'"](.*?)[\'"]\)', text)

    @staticmethod
    def match(pattern, text):
        cpattern = re.compile(pattern)
        match = cpattern.search(text)
        if match:
            return match.group(1)
        return None


class SinaSessionLoginer(object):
    "Login implements the weibo login process."

    def __init__(self, session):
        """
        Input:
        - session: A requests.Session object.
        """
        self.prelogin_url = "http://login.sina.com.cn/sso/prelogin.php?entry=weibo" \
            "&callback=sinaSSOController.preloginCallBack&su=&rsakt" \
            "=mod&client=ssologin.js(v1.4.18)&_=1407721000736"
        self.login_url = "http://login.sina.com.cn/sso/login.php?client=ssologin.js(v1.4.18)"
        self.user_identity = None
        self.session = session

    def _encrypt(self, server_time, nonce, pubkey, rsakv):
        """
        Encrypt uesr identifier using pubkey via RSA.

        Return a dict of encrypted user identification for login.
        """
        user_name_base64 = base64.encodebytes(
            self.user_identity.name.encode('utf-8'))[:-1]
        pubkey = int(pubkey, 16)
        key = rsa.PublicKey(pubkey, 65537)
        message = str(server_time) + '\t' + str(nonce) + '\n' + \
            str(self.user_identity.pwd)  # 拼接明文加密文件中得到
        passwd = rsa.encrypt(message.encode('utf-8'), key)  # 加密
        passwd = binascii.b2a_hex(passwd)
        ret = {
            'entry': 'weibo',
            'gateway': '1',
            'from': '',
            'savestate': '7',
            'userticket': '1',
            'ssosimplelogin': '1',
            'vsnf': '1',
            'vsnval': '',
            'su': user_name_base64,
            'service': 'miniblog',
            'servertime': server_time,
            'nonce': nonce,
            'pwencode': 'rsa2',
            'sp': passwd,
            'encoding': 'UTF-8',
            'prelt': '115',
            'rsakv': rsakv,
            'url': 'http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack',
            'returntype': 'META'
        }
        logger.debug('Encrypted payload is: %s', ret)
        return ret

    def login(self, user_identity):
        """
        Login via the specified user identity and store the cookies to the
        specified session.
        """
        self.user_identity = user_identity
        # Phase one: Retrieve server time and public key for user identifier
        # encryption.
        logger.info('Login Phase 1: retrieve server time and public key')
        res = self.session.get(self.prelogin_url)
        logger.debug('Get prelogin response: %s', res.text)
        json_str = ResponseUtils.parseJson(res.text)
        json_data = json.loads(json_str)
        server_time = json_data['servertime']
        nonce = json_data['nonce']
        pubkey = json_data['pubkey']
        rsakv = json_data['rsakv']
        encrypt_payload = self._encrypt(server_time, nonce, pubkey, rsakv)

        # Phase two: Send the encrypted user identifier to login_url.
        logger.info('Login Phase 2: send encrypted user identifier')
        res = self.session.post(self.login_url, encrypt_payload)
        logger.debug('Get login response: %s', res.text)
        logger.debug('Get login cookies: %s', res.cookies)
        logger.debug('Session cookies: %s', self.session.cookies)
        redirect_url = ResponseUtils.parseRedirectUrl(res.text)

        # Phase three: Get the final cookies.
        logger.info('Login Phase 3: redirect and get final cookies')
        res = self.session.get(redirect_url)
        logger.debug('Get redirect response: %s', res.text)
        logger.debug('Get login cookies: %s', res.cookies)
        logger.debug('Session cookies: %s', self.session.cookies)
        logger.info('Login success')
