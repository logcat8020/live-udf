package com.red.utils;

/**
 * @author chenhuiup
 * @create 2024-04-01 16:50
 */
public class MarkdownBuilder {
    private StringBuilder builder;

    public MarkdownBuilder() {
        builder = new StringBuilder();
    }

    public MarkdownBuilder header1(String text) {
        builder.append("# ").append(text).append("\n\n");
        return this;
    }

    public MarkdownBuilder header2(String text) {
        builder.append("## ").append(text).append("\n\n");
        return this;
    }

    public MarkdownBuilder header3(String text) {
        builder.append("### ").append(text).append("\n\n");
        return this;
    }

    public MarkdownBuilder paragraph(String text) {
        builder.append(text).append("\n\n");
        return this;
    }

    public MarkdownBuilder unorderedList(String... items) {
        for (String item : items) {
            builder.append("* ").append(item).append("\n");
        }
        builder.append("\n");
        return this;
    }

    public MarkdownBuilder orderedList(String... items) {
        for (int i = 0; i < items.length; i++) {
            builder.append(i + 1).append(". ").append(items[i]).append("\n");
        }
        builder.append("\n");
        return this;
    }

    public MarkdownBuilder splitLine(){

        builder.append("---\n");
        return this;
    }

    public MarkdownBuilder table(String[] headers, String[][] data) {
        // 添加表头
        for (String header : headers) {
            builder.append("| ").append(header).append(" ");
        }
        builder.append("|\n");

        // 添加分隔线
        for (int i = 0; i < headers.length; i++) {
            builder.append("| --- ");
        }
        builder.append("|\n");

        // 添加数据
        for (String[] row : data) {
            for (String cell : row) {
                builder.append("| ").append(cell).append(" ");
            }
            builder.append("|\n");
        }
        builder.append("\n");

        return this;
    }

    public String build() {
        return builder.toString();
    }
}

