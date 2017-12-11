from sinaspider.sina_login import *


def test_response_utils_parse_json():
    json_str = '{"retcode":0,"servertime":1512395715,"pcid":"tc-b4680a6d9e6788e933937b2b206f3f4a79a6","nonce":"JX2OZ5","pubkey":"EB2A38568661887FA180BDDB5CABD5F21C7BFD59C090CB2D245A87AC253062882729293E5506350508E7F9AA3BB77F4333231490F915F6D63C55FE2F08A49B353F444AD3993CACC02DB784ABBB8E42A9B1BBFFFB38BE18D78E87A0E41B9B8F73A928EE0CCEE1F6739884B9777E4FE9E88A1BBE495927AC4A799B3181D6442443","rsakv":"1330428213","exectime":7}'
    assert ResponseUtils.parseJson('test (' + json_str + ')') == json_str


def test_response_utils_parse_redirect_url():
    url = 'http://weibo.com'
    wrap_url = 'location.replace("' + url + '")'
    assert ResponseUtils.parseRedirectUrl('test :' + wrap_url) == url
