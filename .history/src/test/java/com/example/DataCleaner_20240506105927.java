package com.example;

import java.io.*;
import java.util.*;
import org.apache.commons.csv.*;

public class DataCleaner {
    public static void main(String[] args) throws IOException {
        // Đường dẫn đến các tệp CSV
        String file1Path = "data/computed_insight_success_of_active_sellers.csv";
        String file2Path = "data/summer-products-with-rating-and-performance_2020-08.csv";
        String file3Path = "data/unique-categories.csv";
        String file4Path = "data/unique-categories.sorted-by-count.csv";

        // Tải dữ liệu từ các tệp CSV
        List<Map<String, String>> df1 = loadCSV(file1Path);
        List<Map<String, String>> df2 = loadCSV(file2Path);
        List<Map<String, String>> df3 = loadCSV(file3Path);
        List<Map<String, String>> df4 = loadCSV(file4Path);

        // Làm sạch và lưu dữ liệu đã được làm sạch
        List<Map<String, String>> df1Clean = cleanData(df1);
        List<Map<String, String>> df2Clean = cleanData(df2);
        List<Map<String, String>> df3Clean = cleanData(df3);
        List<Map<String, String>> df4Clean = cleanData(df4);

        // Lưu dữ liệu đã làm sạch vào các tệp mới
        saveCSV(df1Clean, "data/computed_insight_success_of_active_sellers_cleaned.csv");
        saveCSV(df2Clean, "data/summer-products-with-rating-and-performance_2020-08_cleaned.csv");
        saveCSV(df3Clean, "data/unique-categories_cleaned.csv");
        saveCSV(df4Clean, "data/unique-categories.sorted-by-count_cleaned.csv");

        System.out.println("Dữ liệu đã được làm sạch và lưu thành công.");
    }

    // Hàm tải tệp CSV và trả về danh sách các bản ghi
    private static List<Map<String, String>> loadCSV(String filePath) throws IOException {
        List<Map<String, String>> data = new ArrayList<>();
        try (Reader in = new FileReader(filePath)) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
            for (CSVRecord record : records) {
                Map<String, String> row = new HashMap<>();
                for (String header : record.toMap().keySet()) {
                    row.put(header, record.get(header));
                }
                data.add(row);
            }
        }
        return data;
    }

    // Hàm lưu danh sách bản ghi vào tệp CSV
    private static void saveCSV(List<Map<String, String>> data, String filePath) throws IOException {
        if (data.isEmpty())
            return;
        try (Writer out = new FileWriter(filePath)) {
            CSVPrinter printer = new CSVPrinter(out,
                    CSVFormat.DEFAULT.withHeader(data.get(0).keySet().toArray(new String[0])));
            for (Map<String, String> row : data) {
                printer.printRecord(row.values());
            }
        }
    }

    // Hàm loại bỏ dữ liệu trùng lặp
    private static List<Map<String, String>> removeDuplicates(List<Map<String, String>> data) {
        Set<Map<String, String>> uniqueData = new HashSet<>(data);
        return new ArrayList<>(uniqueData);
    }

    // Hàm loại bỏ các cột không cần thiết
    private static List<Map<String, String>> removeIrrelevantColumns(List<Map<String, String>> data) {
        if (data.isEmpty())
            return data;
        List<String> headers = new ArrayList<>(data.get(0).keySet());
        Map<String, Set<String>> uniqueValues = new HashMap<>();
        Map<String, Boolean> allMissing = new HashMap<>();

        // Khởi tạo map
        for (String header : headers) {
            uniqueValues.put(header, new HashSet<>());
            allMissing.put(header, true);
        }

        // Đánh dấu các giá trị duy nhất và kiểm tra giá trị thiếu
        for (Map<String, String> row : data) {
            for (String header : headers) {
                String value = row.get(header);
                if (value != null && !value.isEmpty()) {
                    uniqueValues.get(header).add(value);
                    allMissing.put(header, false);
                }
            }
        }

        // Xác định các cột không cần thiết
        List<String> irrelevantColumns = new ArrayList<>();
        for (String header : headers) {
            if (uniqueValues.get(header).size() == 1 || allMissing.get(header)) {
                irrelevantColumns.add(header);
            }
        }

        // Loại bỏ các cột không cần thiết
        List<Map<String, String>> cleanedData = new ArrayList<>();
        for (Map<String, String> row : data) {
            Map<String, String> cleanedRow = new HashMap<>(row);
            for (String column : irrelevantColumns) {
                cleanedRow.remove(column);
            }
            cleanedData.add(cleanedRow);
        }
        return cleanedData;
    }

    // Hàm loại bỏ các bản ghi có giá trị ngoại lai
    private static List<Map<String, String>> removeOutliers(List<Map<String, String>> data,
            List<String> numericColumns) {
        if (data.isEmpty())
            return data;
        for (String column : numericColumns) {
            List<Double> values = new ArrayList<>();
            for (Map<String, String> row : data) {
                try {
                    values.add(Double.parseDouble(row.get(column)));
                } catch (NumberFormatException | NullPointerException ignored) {
                }
            }
            if (values.isEmpty())
                continue;
            double q1 = getPercentile(values, 25);
            double q3 = getPercentile(values, 75);
            double iqr = q3 - q1;
            double lowerBound = q1 - 1.5 * iqr;
            double upperBound = q3 + 1.5 * iqr;

            data.removeIf(row -> {
                try {
                    double value = Double.parseDouble(row.get(column));
                    return value < lowerBound || value > upperBound;
                } catch (NumberFormatException | NullPointerException e) {
                    return false;
                }
            });
        }
        return data;
    }

    // Hàm tính toán giá trị phần trăm (percentile)
    private static double getPercentile(List<Double> values, double percentile) {
        Collections.sort(values);
        int index = (int) Math.ceil(percentile / 100.0 * values.size()) - 1;
        return values.get(index);
    }

    // Hàm loại bỏ dữ liệu bị thiếu
    private static List<Map<String, String>> removeMissingData(List<Map<String, String>> data) {
        return new ArrayList<>(data.stream()
                .filter(row -> row.values().stream().noneMatch(String::isEmpty))
                .toList());
    }

    // Hàm loại bỏ dữ liệu không liên quan và lỗi cấu trúc
    private static List<Map<String, String>> cleanData(List<Map<String, String>> data) {
        List<Map<String, String>> cleanedData = removeDuplicates(data);
        cleanedData = removeIrrelevantColumns(cleanedData);
        cleanedData = removeOutliers(cleanedData, Arrays.asList(
                "totalunitssold", "meanunitssoldperproduct", "rating",
                "merchantratingscount", "meanretailprices", "meanproductratingscount"));
        cleanedData = removeMissingData(cleanedData);
        return cleanedData;
    }
}
