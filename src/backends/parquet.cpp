/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#include "./parquet.h"

#include <omp.h>

#include <arrow/io/file.h>
#include <parquet/stream_writer.h>

#include <iostream>

using parquet::WriterProperties;
using parquet::ParquetVersion;
using parquet::ParquetDataPageVersion;
using arrow::Compression;

static void write_frame(const char *outfile, std::shared_ptr<parquet::schema::GroupNode> schema,
                        char *buffer);

int backend_parquet(struct smog_tracefile *tracefile, const char *path) {
    // check the outfile pattern
    if (!strstr(path, "%s")) {
        std::cerr << "error: OUTFILE must contain '%s'" << std::endl;
        return 1;
    }

    // create a parquet schema
    parquet::schema::NodeVector fields;

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

    std::cout << "Unpacking parquet files:  0%" << std::flush;

    size_t total_work = tracefile->num_frames;
    size_t work_done = 0;

    #pragma omp parallel for
    for (size_t i = 0; i < tracefile->num_frames; ++i) {
        write_frame(path, schema, tracefile->buffer + tracefile->frame_offsets[i]);

        // progress reporting on the last thread
        if (omp_get_thread_num() == omp_get_num_threads() - 1) {
            work_done++;
            std::cout << "\rUnpacking parquet files:  "
                      << work_done * omp_get_num_threads() * 100 / total_work
                      << "%"
                      << std::flush;
        }
    }

    std::cout << "\rUnpacking parquet files:  100%" << std::endl;

    return 0;
}

static void write_frame(const char *outfile, std::shared_ptr<parquet::schema::GroupNode> schema,
                        char *buffer) {
    // extract the timeval from the frame
    time_t sec = *(uint32_t*)buffer;
    uint32_t usec = *(uint32_t*)(buffer + 4);

    struct tm tm;
    localtime_r(&sec, &tm);

    // produce a string representation of the time
    char timestr[27];
    size_t len = strftime(timestr, 20, "%F_%T", &tm);
    if (len == 0 || len >= 20) {
        std::cerr << "failed to create output filename" << std::endl;
        return;
    }
    snprintf(timestr + len, 8, ".%06u", usec);

    // produce a path to the output file
    int n = snprintf(NULL, 0, outfile, timestr);
    char *outfile_buf = (char*)malloc(n);
    if (outfile_buf == NULL) {
        std::cerr << "failed to allocate memory" << std::endl;
        return;
    }
    snprintf(outfile_buf, n + 1, outfile, timestr);

    // open the output stream
    std::shared_ptr<arrow::io::FileOutputStream> outstream;

    PARQUET_ASSIGN_OR_THROW(
        outstream,
        arrow::io::FileOutputStream::Open(outfile_buf));

    parquet::WriterProperties::Builder builder;
    builder
       .max_row_group_length(64 * 1024)
       ->created_by("smog-meter")
       ->version(ParquetVersion::PARQUET_2_6)
       ->data_page_version(ParquetDataPageVersion::V2)
       ->compression(Compression::SNAPPY);

    // create the stream writer
    parquet::StreamWriter out {
        parquet::ParquetFileWriter::Open(outstream, schema, builder.build())
    };

    // extract the number of VMAs from the frame
    uint32_t num_vmas = *(uint32_t*)(buffer + 8);

    size_t index = 12;
    for (size_t i = 0; i < num_vmas; ++i) {
        uint64_t start = *(uint64_t*)(buffer + index);
        uint64_t end = *(uint64_t*)(buffer + index + 8);
        index += 16;

        uint32_t pages = *(uint32_t*)(buffer + index);
        index += 4;

        if (end != start + pages) {
            std::cerr << "warning: mismatched VMA range" << std::endl;
        }

        size_t words = (pages * 2 + (32 - 1)) / 32;

        for (size_t j = 0; j < pages; ++j) {
            bool is_present = (buffer[j / 16] >> ((j % 16) * 2)) & 0x1;
            bool is_dirty   = (buffer[j / 16] >> ((j % 16) * 2 + 1)) & 0x1;
            uint64_t pageno = start + j;

            out << pageno << is_present << is_dirty << parquet::EndRow;
        }

        index += words * 4;
    }

    // cleanup
    free(outfile_buf);
}
