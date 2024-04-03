import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

class CsvFileSorter {
    private static final int ROW_BUFFER_SIZE = 10000;

    private String delimiter = ",";
    private Pattern delimiterPattern;
    private final Queue<Path> tempFiles = new LinkedList<>();

    public CsvFileSorter() {
        initDelimiterPattern();
    }

    public CsvFileSorter(String delimiter) {
        this.delimiter = delimiter;
        initDelimiterPattern();
    }

    private void initDelimiterPattern() {
        delimiterPattern = Pattern.compile(delimiter);
    }

    public void sort(Path inputFilePath, Path outputFilePath) throws IOException {
        try {
            var lines = new ArrayList<String>(ROW_BUFFER_SIZE);

            try (BufferedReader bufferedReader = Files.newBufferedReader(inputFilePath)) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    lines.add(line);

                    if (lines.size() == ROW_BUFFER_SIZE) {
                        createNewSortedChunk(lines);
                        lines.clear();
                    }
                }
            }

            if (!lines.isEmpty()) {
                createNewSortedChunk(lines);
            }

            if (tempFiles.isEmpty()) {
                Files.createFile(outputFilePath);
                return;
            }

            while (tempFiles.size() > 1) {
                mergeChunks(tempFiles.poll(), tempFiles.poll());
            }

            Path sortedFile = tempFiles.remove();
            Files.copy(sortedFile, outputFilePath, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            cleanTempFiles();
        }
    }

    public void createNewSortedChunk(List<String> unsortedLines) throws IOException {
        Path tempFile = Files.createTempFile("sortedPart", null);
        tempFiles.add(tempFile);
        unsortedLines.sort(Comparator.comparing(this::getKeyForLine));

        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(tempFile, StandardOpenOption.WRITE)) {
            for (String line : unsortedLines) {
                bufferedWriter.append(line).append("\n");
            }
        }
    }

    public void mergeChunks(Path file1, Path file2) throws IOException {
        Path tempFile = Files.createTempFile("sortedPart", null);
        tempFiles.add(tempFile);

        try (BufferedReader bufferedReader1 = Files.newBufferedReader(file1);
             BufferedReader bufferedReader2 = Files.newBufferedReader(file2);
             BufferedWriter bufferedWriter = Files.newBufferedWriter(tempFile, StandardOpenOption.WRITE)) {

            String line1 = bufferedReader1.readLine();
            String line2 = bufferedReader2.readLine();
            Integer key1 = getKeyForLine(line1);
            Integer key2 = getKeyForLine(line2);

            while (line1 != null && line2 != null) {
                if (key1 <= key2) {
                    bufferedWriter.append(line1).append("\n");
                    line1 = bufferedReader1.readLine();
                    key1 = getKeyForLine(line1);
                } else {
                    bufferedWriter.append(line2).append("\n");
                    line2 = bufferedReader2.readLine();
                    key2 = getKeyForLine(line2);
                }
            }

            BufferedReader remainderReader = line1 == null ? bufferedReader2 : bufferedReader1;
            String line = line1 == null ? line2 : line1;

            while (line != null) {
                bufferedWriter.append(line).append("\n");
                line = remainderReader.readLine();
            }
        }

        Files.delete(file1);
        Files.delete(file2);
    }

    private Integer getKeyForLine(String line) {
        if (line == null || line.isEmpty()) {
            return null;
        }

        return Integer.parseInt(delimiterPattern.split(line, 2)[0]);
    }

    public void cleanTempFiles() {
        for (Path tempFile : tempFiles) {
            try {
                Files.delete(tempFile);
            } catch (IOException e) {
                // suppress to try clean other files
            }
        }
    }
}