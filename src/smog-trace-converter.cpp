/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>

#include "arrow/io/file.h"
#include "parquet/stream_writer.h"

using parquet::WriterProperties;
using parquet::ParquetVersion;
using parquet::ParquetDataPageVersion;
using arrow::Compression;

#define checked_lseek(...) do { \
    int res = lseek(__VA_ARGS__); \
    if (res < 0) { \
        fprintf(stderr, "%s: ", tracefile); \
        perror("lseek"); \
        return 1; \
    } \
} while (0)

#define checked_read(FD, BUF, N) do { \
    int res = read((FD), (BUF), (N)); \
    if (res < 0) { \
        fprintf(stderr, "%s: ", tracefile); \
        perror("read"); \
        return 1; \
    } \
    if (res != N) { \
        fprintf(stderr, "%s: insufficient data read", tracefile); \
        return 1; \
    } \
} while (0)

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "usage: %s <TRACEFILE> <OUTFILE>\n", argv[0]);
        return 2;
    }

    char *tracefile = argv[1];
    char *outfile = argv[2];

    int fd = open(tracefile, O_RDONLY);

    size_t num_frames = 0;
    size_t num_vmas = 0;
    size_t num_pages = 0;

    while (1) {
        checked_lseek(fd, 8, SEEK_CUR);

        uint32_t vmas;
        int res = read(fd, &vmas, 4);

        if (res == 0) {
            break;
        }

        if (res != 4) {
            fprintf(stderr, "%s: ", tracefile);
            perror("read");
            return 1;
        }

        num_vmas += vmas;

        for (size_t i = 0; i < vmas; ++i) {
            checked_lseek(fd, 16, SEEK_CUR);

            uint32_t pages;
            checked_read(fd, &pages, 4);
            num_pages += pages;

            size_t words = (pages * 2 + (32 - 1)) / 32;
            checked_lseek(fd, words * 4, SEEK_CUR);
        }

        num_frames++;
    }

    printf("Unpacking Tracefile with %zu Frames and %zu VMAS and %zu Pages\n", num_frames, num_vmas, num_pages);

    std::shared_ptr<arrow::io::FileOutputStream> outstream;

    PARQUET_ASSIGN_OR_THROW(
        outstream,
        arrow::io::FileOutputStream::Open(outfile));

    parquet::WriterProperties::Builder builder;
    builder
       .max_row_group_length(64 * 1024)
       ->created_by("smog-meter")
       ->version(ParquetVersion::PARQUET_2_6)
       ->data_page_version(ParquetDataPageVersion::V2)
       ->compression(Compression::SNAPPY);

    std::shared_ptr<arrow::Field> field_timestamp, field_vma_id, field_pageno, field_is_present, field_is_dirty;

    parquet::schema::NodeVector fields;

    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "timestamp", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE,
        parquet::ConvertedType::NONE));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "vma_id", parquet::Repetition::REQUIRED, parquet::Type::INT32,
        parquet::ConvertedType::UINT_32));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "pageno", parquet::Repetition::REQUIRED, parquet::Type::INT64,
        parquet::ConvertedType::UINT_64));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "is_present", parquet::Repetition::REQUIRED, parquet::Type::BOOLEAN,
        parquet::ConvertedType::NONE));
    fields.push_back(parquet::schema::PrimitiveNode::Make(
        "is_dirty", parquet::Repetition::REQUIRED, parquet::Type::BOOLEAN,
        parquet::ConvertedType::NONE));

    auto schema = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));

    parquet::StreamWriter out {
        parquet::ParquetFileWriter::Open(outstream, schema, builder.build())
    };

    checked_lseek(fd, 0, SEEK_SET);

    for (size_t frame = 0; frame < num_frames; ++frame) {
        printf("  %zu / %zu\n", frame + 1, num_frames);

        uint32_t sec, usec;
        checked_read(fd, &sec, 4);
        checked_read(fd, &usec, 4);
        double timestamp = sec + usec / 1000000.0;

        uint32_t vmas;
        checked_read(fd, &vmas, 4);

        for (size_t i = 0; i < vmas; ++i) {
            uint64_t start, end;
            checked_read(fd, &start, 8);
            checked_read(fd, &end, 8);

            uint32_t pages;
            checked_read(fd, &pages, 4);

            size_t words = (pages * 2 + (32 - 1)) / 32;

            static uint32_t *buffer = NULL;
            static size_t buflen = 0;
            if (buflen < words) {
                free(buffer);
                buflen = words;
                buffer = (uint32_t*)malloc(sizeof(*buffer) * words);
                if (!buffer) {
                    perror("malloc");
                    return 1;
                }
            }
            checked_read(fd, buffer, words * sizeof(*buffer));

            for (size_t j = 0; j < pages; ++j) {
                bool is_present = (buffer[j / 16] >> ((j % 16) * 2)) & 0x1;
                bool is_dirty   = (buffer[j / 16] >> ((j % 16) * 2 + 1)) & 0x1;
                uint32_t vma_id = i;
                uint64_t pageno = start + j;

                out << timestamp << vma_id << pageno << is_present << is_dirty << parquet::EndRow;
            }
        }
    }
}
