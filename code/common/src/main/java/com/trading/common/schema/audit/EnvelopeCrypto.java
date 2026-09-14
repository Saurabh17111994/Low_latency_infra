package com.trading.common.schema.audit;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Objects;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES-256-GCM envelope encryption for the encrypted export pipeline: every
 * export bundle gets a fresh 256-bit data key; the payload is sealed with the
 * data key (AAD = the record identity), and the data key itself is wrapped
 * with the master key for the bundle's key version. The plaintext data key is
 *
 * <p>Every key used here must be exactly {@link #AES_KEY_BYTES} bytes. The JDK
 * accepts 16- and 24-byte AES keys, so a shortened key would silently run
 * AES-128/192-GCM under a name promising AES-256 — a key-length check rejects
 * it on both the seal and the open path (P6-283, P6-284). The AAD is the record
 * identity and must not be empty: GCM without AAD does not bind the bundle to
 * its record (P6-675).
 *
 * <p>Inputs are validated before the cipher is touched and only
 * {@link GeneralSecurityException} is wrapped, so a programming error (null
 * key, empty AAD, truncated payload) raises its own exception instead of
 * masquerading as a tamper finding (P6-675, P6-676, P6-677).
 *
 * <p>JDK-only (no BouncyCastle). AEAD: 128-bit tag, 96-bit IV.
 */
public final class EnvelopeCrypto {

    private EnvelopeCrypto() {}

    private static final String TRANSFORM = "AES/GCM/NoPadding";
    private static final int TAG_BITS = 128;
    private static final int TAG_BYTES = TAG_BITS / 8;
    private static final int IV_BYTES = 12;

    /** Key length for AES-256, required of every key this class accepts. */
    public static final int AES_KEY_BYTES = 32;

    private static final SecureRandom RNG = new SecureRandom();

    /** A fresh 256-bit data key. */
    public static byte[] newDataKey() {
        byte[] key = new byte[AES_KEY_BYTES];
        RNG.nextBytes(key);
        return key;
    }

    /** Sealed payload layout: {@code iv(12) || ciphertext || tag(16)}. */
    public static byte[] seal(byte[] dataKey, byte[] plaintext, byte[] aad) {
        requireAes256Key(dataKey, "dataKey");
        Objects.requireNonNull(plaintext, "plaintext");
        requireRecordIdentityAad(aad);
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORM);
            byte[] iv = new byte[IV_BYTES];
            RNG.nextBytes(iv);
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dataKey, "AES"),
                    new GCMParameterSpec(TAG_BITS, iv));
            cipher.updateAAD(aad);
            byte[] ct = cipher.doFinal(plaintext);
            byte[] out = new byte[IV_BYTES + ct.length];
            System.arraycopy(iv, 0, out, 0, IV_BYTES);
            System.arraycopy(ct, 0, out, IV_BYTES, ct.length);
            return out;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM seal failed", e);
        }
    }

    /** Reverse of {@link #seal}. Throws (fail-closed) on wrong key / tamper / bad AAD. */
    public static byte[] open(byte[] dataKey, byte[] sealed, byte[] aad) {
        requireAes256Key(dataKey, "dataKey");
        requireRecordIdentityAad(aad);
        if (sealed == null || sealed.length < IV_BYTES + TAG_BYTES) {
            throw new IllegalArgumentException("sealed payload too short: need iv(" + IV_BYTES
                    + ")+tag(" + TAG_BYTES + ") at minimum, got "
                    + (sealed == null ? "null" : sealed.length + " bytes"));
        }
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORM);
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dataKey, "AES"),
                    new GCMParameterSpec(TAG_BITS, sealed, 0, IV_BYTES));
            cipher.updateAAD(aad);
            return cipher.doFinal(sealed, IV_BYTES, sealed.length - IV_BYTES);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-GCM open failed (wrong key, tamper, or AAD)",
                    e);
        }
    }

    /** Wrap a data key with a master key; same {@code iv || ciphertext || tag} layout. */
    public static byte[] wrap(byte[] masterKey, byte[] dataKey, byte[] aad) {
        return seal(masterKey, dataKey, aad);
    }

    /** Unwrap a wrapped data key with the master key for its version. */
    public static byte[] unwrap(byte[] masterKey, byte[] wrapped, byte[] aad) {
        return open(masterKey, wrapped, aad);
    }

    private static void requireAes256Key(byte[] key, String name) {
        if (key == null || key.length != AES_KEY_BYTES) {
            throw new IllegalArgumentException(name + " must be " + AES_KEY_BYTES
                    + " bytes for AES-256, got "
                    + (key == null ? "null" : key.length + " bytes"));
        }
    }

    /** AAD carries the record identity; an empty AAD leaves the bundle unbound. */
    private static void requireRecordIdentityAad(byte[] aad) {
        if (aad == null || aad.length == 0) {
            throw new IllegalArgumentException(
                    "aad must be the non-empty record identity — an unbound bundle is not evidence");
        }
    }
}
