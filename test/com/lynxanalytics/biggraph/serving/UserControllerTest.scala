package com.lynxanalytics.biggraph.serving

import org.scalatest.FunSuite

class UserControllerTest extends FunSuite {
  test("signature from Python can be verified") {
    val publicKeyBase64 = """
      MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAykDWYB9jAJzqxmcRYTfMScRCFUj+NaF1K3qikyMg5L1+I67d
      YuHaey4/9D4pqZwIuDdxRa9lC5tvZJZplsWPdAJG1PpLraCuuOfpg2XeZ5HmZR1YLSA9fquH9nsTPaSJARnm2WHFXhsb
      EExlnSp5s5N41gK2kHtFSYQv6C4yV3IbzylaKhPXNzP7kpm1VQ19Cb6pg0QsulYIEZJE5gtwDpxasyNZV0IY42vomqPu
      6CSAkVhACek3lTEDbLuaNom+iRwOmZ5JWTHbsOlvCjLoMjNdtmtk70ZO1MrTlv3S/8vupkAzEbv7P5TsYKwvR85tRFJ+
      lrM1SS5NadgGX3a9sQIDAQAB"""
      .replace(" ", "").replace("\n", "")
    val message = "test message"
    val signatureBase64 = """
      PfdT9CZlceA09pspvihb7GL3YQDqMvHNPvhDeRrLZHvgiuKttB5LDintbtIjxSOtlfSNtV9YBuc8rxAG8Dhgnf9fmUhP
      Ls/OUrR7yQLBYalIdC331wsgAfuZYJCov4axrzcaKjC1JqRm/aaoYvQIw/gNXy+pqrpcxIkd17StDTE4oGPaJEUDcoNl
      ulrSTW2K0cdCwKi0VQCBZWYw5jCX2Oqlh5eMYj+uGnt+IePvEJXAVzdZigUdy8bRUgW3fy/7jyY2ti7TTkbhjbOmUZyw
      V7fe0/uy+hRJm/Qv+fRk4Xo7Vm4+aRCSrMTCzpqOnwj4ppA5W2FlYTKPY6IezSaAfQ=="""
      .replace(" ", "").replace("\n", "")
    val publicKey = UserController.loadPublicKey(publicKeyBase64)
    assert(UserController.isValidSignature(message, signatureBase64, publicKey))
  }
}
