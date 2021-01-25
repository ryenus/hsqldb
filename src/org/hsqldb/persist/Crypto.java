/* Copyright (c) 2001-2021, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb.persist;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringConverter;

/**
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.4.1
 * @since 1.9.0
 */

// support for IV parameters added by Shaun Murphy (shaunmurphy@users dot sourcedorge.net)
public class Crypto {

    final SecretKeySpec   key;
    final Cipher          outCipher;
    final Cipher          inCipher;
    final Cipher          inStreamCipher;
    final Cipher          outStreamCipher;
    final IvParameterSpec ivSpec;

    public Crypto(String keyString, String ivString, String cipherName,
                  String provider) {

        final String keyAlgorithm = (cipherName.contains("/"))
                                    ? cipherName.substring(0,
                                        cipherName.indexOf("/"))
                                    : cipherName;

        try {
            byte[] encodedKey =
                StringConverter.hexStringToByteArray(keyString);

            if (ivString != null && !ivString.isEmpty()) {
                byte[] encodedIv =
                    StringConverter.hexStringToByteArray(ivString);

                ivSpec = new IvParameterSpec(encodedIv);
            } else {
                ivSpec = null;
            }

            key       = new SecretKeySpec(encodedKey, keyAlgorithm);
            outCipher = provider == null ? Cipher.getInstance(cipherName)
                                         : Cipher.getInstance(cipherName,
                                         provider);

            if (ivSpec == null) {
                outCipher.init(Cipher.ENCRYPT_MODE, key);
            } else {
                outCipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
            }

            outStreamCipher = provider == null ? Cipher.getInstance(cipherName)
                                               : Cipher.getInstance(cipherName,
                                               provider);

            if (ivSpec == null) {
                outStreamCipher.init(Cipher.ENCRYPT_MODE, key);
            } else {
                outStreamCipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
            }

            inCipher = provider == null ? Cipher.getInstance(cipherName)
                                        : Cipher.getInstance(cipherName,
                                        provider);

            if (ivSpec == null) {
                inCipher.init(Cipher.DECRYPT_MODE, key);
            } else {
                inCipher.init(Cipher.DECRYPT_MODE, key, ivSpec);
            }

            inStreamCipher = provider == null ? Cipher.getInstance(cipherName)
                                              : Cipher.getInstance(cipherName,
                                              provider);

            if (ivSpec == null) {
                inStreamCipher.init(Cipher.DECRYPT_MODE, key);
            } else {
                inStreamCipher.init(Cipher.DECRYPT_MODE, key, ivSpec);
            }
        } catch (NoSuchPaddingException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (NoSuchAlgorithmException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (InvalidKeyException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (NoSuchProviderException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (IOException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (InvalidAlgorithmParameterException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }

    public synchronized InputStream getInputStream(InputStream in) {

        if (inCipher == null) {
            return in;
        }

        try {
            if (ivSpec == null) {
                inStreamCipher.init(Cipher.DECRYPT_MODE, key);
            } else {
                inStreamCipher.init(Cipher.DECRYPT_MODE, key, ivSpec);
            }

            return new CipherInputStream(in, inStreamCipher);
        } catch (java.security.InvalidKeyException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (InvalidAlgorithmParameterException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }

    public synchronized OutputStream getOutputStream(OutputStream out) {

        if (outCipher == null) {
            return out;
        }

        try {
            if (ivSpec == null) {
                outStreamCipher.init(Cipher.ENCRYPT_MODE, key);
            } else {
                outStreamCipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
            }

            return new CipherOutputStream(out, outStreamCipher);
        } catch (java.security.InvalidKeyException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (InvalidAlgorithmParameterException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }

    public synchronized int decode(byte[] source, int sourceOffset,
                                   int length, byte[] dest, int destOffset) {

        if (inCipher == null) {
            return length;
        }

        try {
            if (ivSpec == null) {
                inCipher.init(Cipher.DECRYPT_MODE, key);
            } else {
                inCipher.init(Cipher.DECRYPT_MODE, key, ivSpec);
            }

            return inCipher.doFinal(source, sourceOffset, length, dest,
                                    destOffset);
        } catch (java.security.InvalidKeyException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (BadPaddingException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (IllegalBlockSizeException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (ShortBufferException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (InvalidAlgorithmParameterException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }

    public synchronized int encode(byte[] source, int sourceOffset,
                                   int length, byte[] dest, int destOffset) {

        if (outCipher == null) {
            return length;
        }

        try {
            if (ivSpec == null) {
                outCipher.init(Cipher.ENCRYPT_MODE, key);
            } else {
                outCipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
            }

            return outCipher.doFinal(source, sourceOffset, length, dest,
                                     destOffset);
        } catch (java.security.InvalidKeyException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (BadPaddingException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (IllegalBlockSizeException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (ShortBufferException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (InvalidAlgorithmParameterException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }

    public static byte[] getNewKey(String cipherName, String provider) {

        try {
            KeyGenerator generator = provider == null
                                     ? KeyGenerator.getInstance(cipherName)
                                     : KeyGenerator.getInstance(cipherName,
                                         provider);
            SecretKey key = generator.generateKey();
            byte[]    raw = key.getEncoded();

            return raw;
        } catch (java.security.NoSuchAlgorithmException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (NoSuchProviderException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }

    public synchronized int getEncodedSize(int size) {

        try {
            return outCipher.getOutputSize(size);
        } catch (IllegalStateException ex) {
            try {
                if (ivSpec == null) {
                    outCipher.init(Cipher.ENCRYPT_MODE, key);
                } else {
                    outCipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
                }

                return outCipher.getOutputSize(size);
            } catch (java.security.InvalidKeyException e) {
                throw Error.error(ErrorCode.X_S0531, e);
            } catch (InvalidAlgorithmParameterException e) {
                throw Error.error(ErrorCode.X_S0531, e);
            }
        }
    }
}
