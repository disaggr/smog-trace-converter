/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <omp.h>

#include <arrow/io/file.h>
#include <parquet/stream_writer.h>

#include <iostream>
#include <cerrno>
#include <cassert>


using parquet::WriterProperties;
using parquet::ParquetVersion;
using parquet::ParquetDataPageVersion;
using arrow::Compression;


static void write_frame(char *outfile, std::shared_ptr<parquet::schema::GroupNode> schema, char *buffer);


class range {
 public:
    size_t lower;
    size_t upper;

    range(size_t start, size_t end) : lower(start), upper(end) {
        // std::cout << "start: " << start << ", end: " << end << std::endl;
        assert(upper >= lower);
    }

    bool operator==(range const& b) const {
        return b.lower == lower && b.upper == upper;
    }

    bool operator!=(range const& b) const {
        return b.lower != lower || b.upper != upper;
    }

    bool operator>(range const& b) const {
        return lower > b.upper;
    }

    bool operator<(range const& b) const {
        return upper < b.lower;
    }

    range operator&(range b) {
        size_t l = std::max(b.lower, lower);
        size_t r = std::min(b.upper, upper);
        if (r < l) {
            return range(0, 0);
        } else {
            return range(l, r);
        }
    }

    range operator|(range b) {
        size_t l = std::min(b.lower, lower);
        size_t r = std::max(b.upper, upper);
        return range(l, r);
    }

    range& operator|=(range b) {
        *this = *this | b;
        return *this;
    }

    friend std::ostream& operator<<(std::ostream &os, const range &r) {
        os << std::hex << "(0x" << r.lower << ", 0x" << r.upper << ")" << std::dec;
        return os;
    }
};



int main(int argc, char *argv[]) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " <TRACEFILE> <OUTFILE-PATTERN>" << std::endl;
        return 2;
    }

    char *tracefile = argv[1];
    char *outfile = argv[2];

    // check the outfile pattern
    if (!strstr(outfile, "%s")) {
        std::cerr << "usage: " << argv[0] << " <TRACEFILE> <OUTFILE-PATTERN>" << std::endl;
        std::cerr << "  note: OUTFILE-PATTERN must contain '%s'" << std::endl;
        return 2;
    }

    // open and map the tracefile for random access
    int fd = open(tracefile, O_RDONLY);
    if (fd == -1) {
        std::cerr << tracefile << ": open: " << strerror(errno) << std::endl;
        return 1;
    }

    struct stat st;
    int res = fstat(fd, &st);
    if (res != 0) {
        std::cerr << tracefile << ": fstat: " << strerror(errno) << std::endl;
        return 1;
    }

    char *buffer = (char*)mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (buffer == MAP_FAILED) {
        std::cerr << tracefile << ": mmap: " << strerror(errno) << std::endl;
        return 1;
    }

    size_t num_frames = 0;
    size_t num_vmas = 0;
    size_t num_pages = 0;

    // parse all frame offsets from the tracefile
    std::vector<off_t> frame_offsets;
    std::vector<range> active_ranges;
    off_t index = 0;
    while (index < st.st_size) {
        frame_offsets.push_back(index);
        num_frames++;

        // skip timestamp
        index += 8;

        // get number of VMAs
        uint32_t vmas = *(uint32_t*)(buffer + index);
        index += 4;
        num_vmas += vmas;

        // for each VMA, get number of pages
        for (uint32_t i = 0; i < vmas; ++i) {
            // get VMA start / end
            uint64_t start = *(uint64_t*)(buffer + index);
            uint64_t end = *(uint64_t*)(buffer + index + 8);
            index += 16;

            range r(start, end - 1);

            // insert VMA into active ranges
            int matched = 0;
            for (size_t j = 0; j < active_ranges.size(); ++j) {
                if (active_ranges[j] < r) {
                    // not there yet
                    continue;
                }

                if ((active_ranges[j] & r) != range(0, 0)) {
                    // overlapping, extend match
                    active_ranges[j] |= r;
                    matched++;
                }

                if (active_ranges[j] > r) {
                    // beyond. insert if not matched yet
                    if (!matched) {
                        active_ranges.insert(active_ranges.begin() + j, r);
                        matched++;
                    }
                    break;
                }
            }
            if (!matched) {
                // completely new, append.
                active_ranges.push_back(r);
            }

            // merge adjancent or overlapping ranges
            for (size_t j = 1; j < active_ranges.size(); ++j) {
                if (((active_ranges[j - 1] & active_ranges[j]) != range(0, 0))
                        || (active_ranges[j - 1].upper == active_ranges[j].lower - 1)) {
                    active_ranges[j - 1] |= active_ranges[j];
                    active_ranges.erase(active_ranges.begin() + j);
                    j--;
                }
            }

            // get number of pages
            uint32_t pages = *(uint32_t*)(buffer + index);
            index += 4;
            num_pages += pages;

            // skip the words needed for encoding the pages
            size_t words = (pages * 2 + (32 - 1)) / 32;
            index += words * 4;
        }
    }

    std::cout << "Tracefile successfully indexed" << std::endl;
    std::cout << "  " << num_frames << " Frames" << std::endl;
    std::cout << "  " << num_vmas << " VMAs" << std::endl;
    std::cout << "  " << num_pages << " Pages" << std::endl;

    std::cout << "Found " << active_ranges.size() << " active VMEM ranges" << std::endl;
    for (size_t i = 0; i < active_ranges.size(); ++i) {
        std::cout << "  " << active_ranges[i] << " :: "
                  << active_ranges[i].upper - active_ranges[i].lower + 1 << " Pages" << std::endl;
    }

    std::cout << "Unpacking " << num_frames << " parquet files... 0%" << std::flush;

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

    size_t total_work = num_frames;
    size_t work_done = 0;

    #pragma omp parallel for
    for (size_t i = 0; i < num_frames; ++i) {
        write_frame(outfile, schema, buffer + frame_offsets[i]);

        // progress reporting on the first thread
        if (omp_get_thread_num() == 0) {
            work_done++;
            std::cout << "\rUnpacking " << num_frames << " parquet files... "
                      << work_done * omp_get_num_threads() * 100 / total_work
                      << "%"
                      << std::flush;
        }
    }

    std::cout << "\rUnpacking " << num_frames << " parquet files... Done." << std::endl;

    // cleanup
    munmap(buffer, st.st_size);
    close(fd);

    return 0;
}

static void write_frame(char *outfile, std::shared_ptr<parquet::schema::GroupNode> schema, char *buffer) {
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
