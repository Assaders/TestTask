import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvFileSorterTest {
    private static final Pattern pattern = Pattern.compile(",");

    @Test
    public void simpleSortTest() throws IOException {
        CsvFileSorter sorter = new CsvFileSorter();
        Path sourceFile = Paths.get("example.csv");
        Path resultFile = Paths.get("result.csv");

        Files.delete(sourceFile);
        Path file = Files.createFile(sourceFile);

        RandomString randomString = new RandomString(8, ThreadLocalRandom.current());

        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(file, StandardOpenOption.WRITE)) {
            for (int i = 0; i < 155000; ++i) {
                bufferedWriter.append(String.valueOf(ThreadLocalRandom.current().nextInt(100000)))
                        .append(",").append(randomString.nextString())
                        .append(",").append(randomString.nextString())
                        .append("\n");
            }
        }

        sorter.sort(sourceFile, resultFile);

        try (BufferedReader reader = Files.newBufferedReader(resultFile)) {
            Integer previousKey = null;
            String line;

            while ((line = reader.readLine()) != null) {
                Integer newKey = getKeyForLine(line);

                if (previousKey != null) {
                    assertTrue(newKey >= previousKey);
                }

                previousKey = newKey;
            }
        }
    }

    private Integer getKeyForLine(String line) {
        return Integer.parseInt(pattern.split(line, 2)[0]);
    }
}