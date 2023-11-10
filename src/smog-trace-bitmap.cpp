/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <iostream>
#include <cerrno>
#include <cassert>
#include <cstring>
#include <cstdint>
#include <vector>

#include <png.h>

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


static void write_frame(unsigned char *pixels, std::vector<range> ranges, size_t width, char *buffer);


int main(int argc, char *argv[]) {
    if (argc < 3) {
        std::cerr << "usage: " << argv[0] << " <TRACEFILE> <BITMAP>" << std::endl;
        return 2;
    }

    char *tracefile = argv[1];
    char *outfile = argv[2];

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

    size_t total_vmem = 0;
    for (size_t i = 0; i < active_ranges.size(); ++i) {
        total_vmem += active_ranges[i].upper - active_ranges[i].lower + 1;
    }

    // prepare output file
    size_t yres = num_frames;
    size_t xres = total_vmem;

    png_structp png = png_create_write_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
    if (!png) {
        std::cerr << outfile << ": png_create_write_struct: " << strerror(errno) << std::endl;
        return 1;
    }

    png_set_user_limits(png, 0x7fffffff, 0x7fffffff);

    png_infop png_info = png_create_info_struct(png);
    if (!png_info) {
        std::cerr << outfile << ": png_create_info_struct: " << strerror(errno) << std::endl;
        return 1;
    }

    FILE *png_fp = fopen(outfile, "wb");
    if (png_fp == NULL) {
        std::cerr << outfile << ": " << strerror(errno) << std::endl;
        return 1;
    }
    png_init_io(png, png_fp);//9
    png_set_IHDR(png,
                 png_info,
                 xres,
                 yres,
                 8 /* depth */,
                 PNG_COLOR_TYPE_RGB,
                 PNG_INTERLACE_NONE,
                 PNG_COMPRESSION_TYPE_BASE,
                 PNG_FILTER_TYPE_BASE);

    png_colorp palette = (png_colorp)png_malloc(png, PNG_MAX_PALETTE_LENGTH * sizeof(png_color));
    if (!palette) {
        std::cerr << outfile << ": png_malloc: " << strerror(errno) << std::endl;
        return 1;
    }

    png_set_PLTE(png, png_info, palette, PNG_MAX_PALETTE_LENGTH);
    png_write_info(png, png_info);
    png_set_packing(png);

    std::cout << "Unpacking " << xres << "x" << yres << " bitmap... 0%" << std::flush;

    unsigned char *pixels = (unsigned char*)malloc(xres * yres * 3 * sizeof(*pixels));
    if (!pixels) {
        std::cerr << outfile << ": " << strerror(errno) << std::endl;
        return 1;
    }

    size_t total_work = num_frames;
    size_t work_done = 0;

    for (size_t i = 0; i < num_frames; ++i) {
        write_frame(pixels + i * xres * 3, active_ranges, xres, buffer + frame_offsets[i]);

        work_done++;
        std::cout << "\rUnpacking " << xres << "x" << yres << " bitmap... "
                  << work_done * 100 / total_work
                  << "%"
                  << std::flush;
    }

    std::cout << "\rUnpacking " << xres << "x" << yres << " bitmap... Done." << std::endl;

    png_bytepp rows = (png_bytepp)png_malloc(png, yres * sizeof(png_bytep));
    for (size_t i = 0; i < yres; ++i)
        rows[i] = (png_bytep)(pixels + (yres - 1 - i) * xres * 3);

    png_write_image(png, rows);
    png_write_end(png, png_info);

    // cleanup
    png_free(png, palette);
    png_destroy_write_struct(&png, &png_info);
    munmap(buffer, st.st_size);
    close(fd);
    fclose(png_fp);

    return 0;
}

static void write_frame(unsigned char *pixels, std::vector<range> ranges, size_t width, char *buffer) {
    // extract the number of VMAs from the frame
    uint32_t num_vmas = *(uint32_t*)(buffer + 8);

    size_t reserved = 0;
    size_t committed = 0;
    size_t accessed = 0;
    size_t softdirty = 0;

    size_t index = 12;
    for (size_t i = 0; i < num_vmas; ++i) {
        uint64_t start = *(uint64_t*)(buffer + index);
        uint64_t end = *(uint64_t*)(buffer + index + 8);
        index += 16;

        size_t pixel_offset = 0;
        for (size_t j = 0; j < ranges.size(); ++j) {
            if (start > ranges[j].upper) {
                pixel_offset += ranges[j].upper - ranges[j].lower + 1;
            } else if (start > ranges[j].lower) {
                pixel_offset += start - ranges[j].lower;
            } else {
                break;
            }
        }

        uint32_t pages = *(uint32_t*)(buffer + index);
        index += 4;

        if (end != start + pages) {
            std::cerr << "warning: mismatched VMA range" << std::endl;
        }

        size_t words = (pages * 2 + (32 - 1)) / 32;

        reserved += pages;

        uint32_t *page_buffer = (uint32_t*)(buffer + index);

        for (size_t j = 0; j < pages; ++j) {
            int value = (page_buffer[j / 16] >> ((j % 16) * 2)) & 0x3;

            if (value >= 0x1) {
                committed++;
            }
            if (value >= 0x2) {
                accessed++;
            }
            if (value >= 0x3) {
                softdirty++;
            }

            size_t pixel = pixel_offset + j;
            //std::cout << pixel_offset << ", " << pixel << std::endl;
            if (pixel >= width) {
                std::cerr << "warning: pixel position out of range" << std::endl;
                continue;
            }

            // not reserved: black            (0, 0, 0)
            // reserved and not present: blue (0, 0, 1)
            // present and not accessed: cyan (0, 1, 1)
            // accessed and not dirty: green  (0, 1, 0)
            // dirty: red                     (1, 0, 0)

            pixels[pixel * 3] = (value >= 0x3) ? 255 : 0;
            pixels[pixel * 3 + 1] = (value >= 0x1 && value <= 0x2) ? 255 : 0;
            pixels[pixel * 3 + 2] = (value >= 0x0 && value <= 0x1) ? 255 : 0;
        }

        index += words * 4;
    }

    //std::cout << "R: " << reserved << ", C: " << committed << ", D: " << softdirty << std::endl;
}
