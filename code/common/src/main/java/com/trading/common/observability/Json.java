package com.trading.common.observability;

import java.util.function.Consumer;

/**
 * Minimal, dependency-free JSON builder for OTLP-shaped records.
 *
 * <p>The builder knows which container it is inside, and the two member
 * syntaxes are not interchangeable: object members carry a key, array elements
 * do not. Mixing them — an unkeyed object inside an object, the shape the old
 * API serialized as {@code {"a":"1",{"b":"2"}}}, or a keyed member inside an
 * array — throws {@link IllegalStateException} instead of emitting a document
 * that no parser will accept.
 *
 * <p>R-045 covers the separator state: a nested container is a value in the
 * enclosing container, so the enclosing separator is written before it opens,
 * and the enclosing container keeps producing separators afterwards.
 */
final class Json {

    private Json() {}

    /** Build a JSON object document; {@code body} adds the members. */
    static String buildObj(Consumer<Builder> body) {
        return new Builder(Kind.OBJECT).render(body).sb.toString();
    }

    /** Build a JSON array document; {@code body} adds the elements. */
    static String buildArr(Consumer<Builder> body) {
        return new Builder(Kind.ARRAY).render(body).sb.toString();
    }

    private enum Kind { OBJECT, ARRAY }

    static final class Builder {
        final StringBuilder sb = new StringBuilder();
        private final Kind kind;
        private boolean first = true;

        Builder(Kind kind) {
            this.kind = kind;
        }

        // ---- object members: every member carries a key ----

        Builder kv(String k, String v) {
            key(k);
            if (v == null) {
                // R-077: a null value must serialize as JSON null, not the
                // empty string escape(null) produces — absent attributes and
                // empty strings are semantically different in telemetry.
                sb.append("null");
            } else {
                sb.append('"').append(escape(v)).append('"');
            }
            return this;
        }

        Builder kv(String k, long v) {
            key(k);
            sb.append(v);
            return this;
        }

        Builder obj(String k, Consumer<Builder> body) {
            key(k);
            return nested(Kind.OBJECT, body);
        }

        Builder arr(String k, Consumer<Builder> body) {
            key(k);
            return nested(Kind.ARRAY, body);
        }

        // ---- array elements: no key ----

        Builder value(String v) {
            elem();
            sb.append(v == null ? "null" : "\"" + escape(v) + "\"");
            return this;
        }

        Builder value(long v) {
            elem();
            sb.append(v);
            return this;
        }

        Builder obj(Consumer<Builder> body) {
            elem();
            return nested(Kind.OBJECT, body);
        }

        Builder arr(Consumer<Builder> body) {
            elem();
            return nested(Kind.ARRAY, body);
        }

        private Builder render(Consumer<Builder> body) {
            sb.append(kind == Kind.OBJECT ? '{' : '[');
            first = true;
            body.accept(this);
            sb.append(kind == Kind.OBJECT ? '}' : ']');
            return this;
        }

        /** The nested container writes into its own buffer, so its separator state is independent. */
        private Builder nested(Kind nesting, Consumer<Builder> body) {
            sb.append(new Builder(nesting).render(body).sb);
            return this;
        }

        /** Object-member prefix: separator, quoted key, colon. */
        private void key(String k) {
            require(Kind.OBJECT, "keyed member");
            sep();
            sb.append('"').append(escape(requireKey(k))).append("\":");
        }

        private void elem() {
            require(Kind.ARRAY, "array element");
            sep();
        }

        private void require(Kind expected, String what) {
            if (kind != expected) {
                throw new IllegalStateException(what + " is not valid inside a JSON "
                        + (kind == Kind.OBJECT ? "object" : "array"));
            }
        }

        private static String requireKey(String k) {
            if (k == null || k.isBlank()) {
                // A null key used to serialize as "" (escape(null) returns ""),
                // colliding with a legitimate empty key and hiding a caller bug.
                throw new IllegalArgumentException("JSON object key must not be null or blank");
            }
            return k;
        }

        private void sep() {
            if (!first) {
                sb.append(',');
            }
            first = false;
        }
    }

    /** RFC 8259 minimal escaping. */
    static String escape(String s) {
        if (s == null) {
            return "";
        }
        StringBuilder o = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': o.append("\\\""); break;
                case '\\': o.append("\\\\"); break;
                case '\n': o.append("\\n"); break;
                case '\r': o.append("\\r"); break;
                case '\t': o.append("\\t"); break;
                case '\b': o.append("\\b"); break;
                case '\f': o.append("\\f"); break;
                default:
                    if (c < 0x20) {
                        // R-218: hand-rolled hex, no per-char String.format.
                        o.append("\\u");
                        appendHex4(o, c);
                    } else {
                        o.append(c);
                    }
            }
        }
        return o.toString();
    }

    private static void appendHex4(StringBuilder o, int v) {
        o.append(HEX[(v >>> 12) & 0xF]);
        o.append(HEX[(v >>> 8) & 0xF]);
        o.append(HEX[(v >>> 4) & 0xF]);
        o.append(HEX[v & 0xF]);
    }

    private static final char[] HEX = "0123456789abcdef".toCharArray();
}
