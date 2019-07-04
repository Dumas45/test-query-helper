package my.balls;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Utils {
    private static final int EOF = -1;

    public static String readFile(String file) throws IOException {
        try (Reader reader = Files.newBufferedReader(Paths.get(file))) {
            StringBuilder sb = new StringBuilder();
            char[] buff = new char[8192];
            int len;
            if (reader.ready()) {
                while ((len = reader.read(buff)) != EOF) {
                    sb.append(buff, 0, len);
                }
            }
            return sb.toString();
        }
    }
}
