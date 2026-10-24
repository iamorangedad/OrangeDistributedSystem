import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 紧凑 JSON。对象用插入顺序，方便和 Python 的日志、正文对照。 */
final class Json {
    private Json() {}

    static Map<String, Object> obj(Object... pairs) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((String) pairs[i], pairs[i + 1]);
        }
        return map;
    }

    static List<Object> list(Object... items) {
        List<Object> list = new ArrayList<>();
        for (Object item : items) {
            list.add(item);
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> asMap(Object value) {
        return value instanceof Map<?, ?> map ? (Map<String, Object>) map : null;
    }

    @SuppressWarnings("unchecked")
    static List<Object> asList(Object value) {
        return value instanceof List<?> list ? (List<Object>) list : null;
    }

    static Object get(Object node, String key) {
        Map<String, Object> map = asMap(node);
        return map == null ? null : map.get(key);
    }

    static boolean has(Object node, String key) {
        Map<String, Object> map = asMap(node);
        return map != null && map.containsKey(key);
    }

    static String text(Object value) {
        return value instanceof String text ? text : null;
    }

    static Long integer(Object value) {
        if (value instanceof Long number) {
            return number;
        }
        if (value instanceof Integer number) {
            return number.longValue();
        }
        return null;
    }

    static boolean truth(Object value) {
        return value instanceof Boolean flag && flag;
    }

    static String stringify(Object value) {
        StringBuilder out = new StringBuilder();
        write(out, value);
        return out.toString();
    }

    static String quote(String value) {
        StringBuilder out = new StringBuilder();
        writeString(out, value);
        return out.toString();
    }

    static Object parse(String text) {
        Parser parser = new Parser(text);
        Object value = parser.read();
        parser.skip();
        if (!parser.done()) {
            throw new IllegalArgumentException("JSON 末尾有多余内容");
        }
        return value;
    }

    private static void write(StringBuilder out, Object value) {
        if (value == null) {
            out.append("null");
        } else if (value instanceof String text) {
            writeString(out, text);
        } else if (value instanceof Boolean || value instanceof Long || value instanceof Integer) {
            out.append(value);
        } else if (value instanceof Double number) {
            if (number.isInfinite() || number.isNaN()) {
                out.append("null");
            } else {
                out.append(number);
            }
        } else if (value instanceof Map<?, ?> map) {
            out.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                writeString(out, String.valueOf(entry.getKey()));
                out.append(':');
                write(out, entry.getValue());
            }
            out.append('}');
        } else if (value instanceof List<?> list) {
            out.append('[');
            boolean first = true;
            for (Object item : list) {
                if (!first) {
                    out.append(',');
                }
                first = false;
                write(out, item);
            }
            out.append(']');
        } else {
            writeString(out, String.valueOf(value));
        }
    }

    private static void writeString(StringBuilder out, String value) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (ch < 0x20) {
                        out.append(String.format("\\u%04x", (int) ch));
                    } else {
                        out.append(ch);
                    }
                }
            }
        }
        out.append('"');
    }

    private static final class Parser {
        private final String text;
        private int index;

        Parser(String text) {
            this.text = text;
        }

        boolean done() {
            return index >= text.length();
        }

        Object read() {
            skip();
            if (done()) {
                throw new IllegalArgumentException("JSON 为空");
            }
            char ch = text.charAt(index);
            if (ch == '{') {
                return readObject();
            }
            if (ch == '[') {
                return readArray();
            }
            if (ch == '"') {
                return readString();
            }
            if (ch == 't' || ch == 'f') {
                return readLiteral();
            }
            if (ch == 'n') {
                expect("null");
                return null;
            }
            return readNumber();
        }

        private Map<String, Object> readObject() {
            index++;
            Map<String, Object> map = new LinkedHashMap<>();
            skip();
            if (peek('}')) {
                index++;
                return map;
            }
            while (true) {
                skip();
                String key = readString();
                skip();
                if (!peek(':')) {
                    throw new IllegalArgumentException("JSON 对象缺少冒号");
                }
                index++;
                map.put(key, read());
                skip();
                if (peek('}')) {
                    index++;
                    return map;
                }
                if (!peek(',')) {
                    throw new IllegalArgumentException("JSON 对象缺少逗号");
                }
                index++;
            }
        }

        private List<Object> readArray() {
            index++;
            List<Object> list = new ArrayList<>();
            skip();
            if (peek(']')) {
                index++;
                return list;
            }
            while (true) {
                list.add(read());
                skip();
                if (peek(']')) {
                    index++;
                    return list;
                }
                if (!peek(',')) {
                    throw new IllegalArgumentException("JSON 数组缺少逗号");
                }
                index++;
            }
        }

        private String readString() {
            if (!peek('"')) {
                throw new IllegalArgumentException("JSON 字符串缺少引号");
            }
            index++;
            StringBuilder out = new StringBuilder();
            while (!done()) {
                char ch = text.charAt(index++);
                if (ch == '"') {
                    return out.toString();
                }
                if (ch != '\\') {
                    out.append(ch);
                    continue;
                }
                if (done()) {
                    break;
                }
                char escaped = text.charAt(index++);
                switch (escaped) {
                    case '"', '\\', '/' -> out.append(escaped);
                    case 'b' -> out.append('\b');
                    case 'f' -> out.append('\f');
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    case 't' -> out.append('\t');
                    case 'u' -> {
                        if (index + 4 > text.length()) {
                            throw new IllegalArgumentException("JSON 转义不完整");
                        }
                        out.append((char) Integer.parseInt(text.substring(index, index + 4), 16));
                        index += 4;
                    }
                    default -> throw new IllegalArgumentException("JSON 转义不认识");
                }
            }
            throw new IllegalArgumentException("JSON 字符串没有结束");
        }

        private Boolean readLiteral() {
            if (text.startsWith("true", index)) {
                index += 4;
                return Boolean.TRUE;
            }
            if (text.startsWith("false", index)) {
                index += 5;
                return Boolean.FALSE;
            }
            throw new IllegalArgumentException("JSON 字面量不认识");
        }

        private void expect(String literal) {
            if (!text.startsWith(literal, index)) {
                throw new IllegalArgumentException("JSON 字面量不认识");
            }
            index += literal.length();
        }

        private Object readNumber() {
            int start = index;
            if (peek('-')) {
                index++;
            }
            while (!done() && Character.isDigit(text.charAt(index))) {
                index++;
            }
            boolean fractional = false;
            if (peek('.')) {
                fractional = true;
                index++;
                while (!done() && Character.isDigit(text.charAt(index))) {
                    index++;
                }
            }
            if (peek('e') || peek('E')) {
                fractional = true;
                index++;
                if (peek('+') || peek('-')) {
                    index++;
                }
                while (!done() && Character.isDigit(text.charAt(index))) {
                    index++;
                }
            }
            String raw = text.substring(start, index);
            if (!fractional) {
                return Long.parseLong(raw);
            }
            return Double.parseDouble(raw);
        }

        private void skip() {
            while (!done() && Character.isWhitespace(text.charAt(index))) {
                index++;
            }
        }

        private boolean peek(char expected) {
            return !done() && text.charAt(index) == expected;
        }
    }
}
