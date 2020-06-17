'''Prints a signed username token for JUPYTERHUB_USER using lk-private.der.'''
import base64
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
import os

username = os.environ['JUPYTERHUB_USER']
key = RSA.importKey(open('lk-private.der', 'rb').read())
h = SHA256.new(username.encode('utf-8'))
signer = PKCS1_v1_5.new(key)
signature = signer.sign(h)
print(username + '|' + base64.b64encode(signature).decode('ascii'))
