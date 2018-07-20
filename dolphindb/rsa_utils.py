import Crypto
import base64
from Crypto.PublicKey import RSA
from cryptography import x509

class RSAUtils(object):
    def __init__(self):
        pass

    def getPublicKey(self, keyCode):
        binkey = self.decodeOpenSSLPublicKey(keyCode)


    def decodeOpenSSLPublicKey(self, instr):
        pempubheader = "-----BEGIN PUBLIC KEY-----"
        pempubfooter = "-----END PUBLIC KEY-----"
        pemstr = instr.strip()
        pemstr = pemstr.replace(pempubheader, "").replace(pempubfooter, "").replaceAll("\\s", "")
        pubstr = pemstr.strip()
        binkey = base64.b64decode(pubstr)
        return binkey
