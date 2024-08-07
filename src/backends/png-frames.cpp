/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#include "backends/png-frames.h"

#include <iostream>
#include <vector>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cassert>

#include <png.h>
#include <omp.h>

#include "./util.h"
#include "./smog-trace-converter.h"

class range {
 public:
    size_t lower;
    size_t upper;

    range(size_t start, size_t end) : lower(start), upper(end) {
        // std::cout << "start: " << start << ", end: " << end << std::endl;
        assert(upper >= lower);
    }

    bool intersects(range const& b) const {
        return this->lower <= b.upper && b.lower <= this->upper;
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

static void write_frame(const char *outfile, std::vector<range> ranges,
                        size_t total_vmem, char *buffer);

int backend_png_frames(struct smog_tracefile *tracefile, const char *path) {
    // check the outfile pattern
    if (!strstr(path, "%s")) {
        std::cerr << "error: OUTFILE must contain '%s'" << std::endl;
        return 1;
    }

    // aggregate address ranges
    std::cout <<"Aggregating VMA Ranges:   " << std::flush;

    std::vector<range> ranges;

    for (size_t i = 0; i < tracefile->num_frames; ++i) {
        off_t index = tracefile->frame_offsets[i];

        // advance the index over the frame
        // skip timestamp
        index += 8;

        // get number of VMAs
        uint32_t num_vmas = *(uint32_t*)(tracefile->buffer + index);
        index += 4;

        // extend the list of ranges by each VMA
        for (uint32_t i = 0; i < num_vmas; ++i) {
            // get VMA start / end
            uint64_t vma_start = *(uint64_t*)(tracefile->buffer + index);
            uint64_t vma_end = *(uint64_t*)(tracefile->buffer + index + 8);
            index += 16;

            size_t pages = vma_end - vma_start;

            // insert VMA into active ranges
            struct range vma = { vma_start, vma_end - 1 };
            if (arguments.verbose > 3) {
                std::cout << "considering range " << vma << std::endl;
            }

            if (vma.lower == 0 && vma.upper == (size_t)-1) {
                continue;
            }

            int matched = 0;
            for (size_t j = 0; j < ranges.size(); ++j) {
                if (vma.lower > ranges[j].upper) {
                    // keep going
                    continue;
                }

                if (ranges[j].intersects(vma)) {
                    // overlapping, extend match
                    if (arguments.verbose > 3) {
                        std::cout << "  extending " << ranges[j];
                    }
                    ranges[j] |= vma;
                    if (arguments.verbose > 3) {
                        std::cout << "-> " << ranges[j] << std::endl;
                    }
                    matched++;
                    continue;
                }

                if (vma.upper < ranges[j].lower) {
                    // beyond. insert if not matched yet
                    if (!matched) {
                        if (arguments.verbose > 3) {
                            std::cout << "  inserting at " << j << std::endl;
                        }
                        ranges.insert(ranges.begin() + j, vma);
                        matched++;
                    }
                    break;
                }
            }
            if (!matched) {
                // completely new, append.
                if (arguments.verbose > 3) {
                    std::cout << "  appending at " << ranges.size() << std::endl;
                }
                ranges.push_back(vma);
            }

            // merge adjancent or overlapping ranges
            for (size_t j = 1; j < ranges.size(); ++j) {
                if (ranges[j - 1].intersects(ranges[j])
                        || ranges[j - 1].upper == ranges[j].lower - 1) {
                    if (arguments.verbose > 3) {
                        std::cout << "  merging " << ranges[j - 1] << ", " << ranges[j];
                    }
                    ranges[j - 1] |= ranges[j];
                    if (arguments.verbose > 3) {
                        std::cout << "-> " << ranges[j - 1] << std::endl;
                    }
                    ranges.erase(ranges.begin() + j);
                    j--;
                }
            }

            if (arguments.verbose > 3) {
                std::cout << ranges.size() << " ranges" << std::endl;
                if (arguments.verbose) {
                    for (size_t i = 0; i < ranges.size(); ++i) {
                        size_t num_pages = ranges[i].upper - ranges[i].lower + 1;
                        std::cout << "  " << ranges[i] << " :: " << num_pages << " Pages, "
                                  << format_size_string(num_pages * arguments.page_size)
                                  << std::endl;
                    }
                }
            }

            // skip over the name
            uint32_t length = *(uint32_t*)(tracefile->buffer + index);
            index += 4 + length;

            // advance the index over the pages
            size_t words = (pages * 2 + (32 - 1)) / 32;
            index += words * 4;
        }
    }

    size_t total_vmem = 0;
    for (size_t i = 0; i < ranges.size(); ++i) {
        total_vmem += ranges[i].upper - ranges[i].lower + 1;
    }

    std::cout << "found " << ranges.size() << " ranges with " << total_vmem << " pages, sized "
              << format_size_string(total_vmem * arguments.page_size) << std::endl;
    if (arguments.verbose) {
        for (size_t i = 0; i < ranges.size(); ++i) {
            size_t num_pages = ranges[i].upper - ranges[i].lower + 1;
            std::cout << "  " << ranges[i] << " :: " << num_pages << " Pages, "
                      << format_size_string(num_pages * arguments.page_size)
                      << std::endl;
        }
    }

    std::cout << "Writing output frames:    0%" << std::flush;

    size_t total_work = tracefile->num_frames;
    size_t work_done = 0;

    #pragma omp parallel for
    for (size_t i = 0; i < tracefile->num_frames; ++i) {
        write_frame(path, ranges, total_vmem, tracefile->buffer + tracefile->frame_offsets[i]);

        // progress reporting on the last thread
        if (omp_get_thread_num() == omp_get_num_threads() - 1) {
            work_done++;
            std::cout << "\rWriting output frames:    " 
                      << work_done * omp_get_num_threads() * 100 / total_work
                      << "%" << std::flush;
        }
    }

    std::cout << "\rWriting output frames:    100%" << std::endl;

    return 0;
}

struct vma {
    off_t index;
    uint64_t lower;
    uint64_t upper;
    size_t num_pages;
};

static int vma_cmp(const void *vma1, const void *vma2) {
    // sort descending
    return ((struct vma*)vma2)->num_pages - ((struct vma*)vma1)->num_pages;
}

static void write_frame(const char *outfile, std::vector<range> ranges,
                        size_t total_vmem, char *buffer) {
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
        std::cerr << "malloc: " << strerror(errno) << std::endl;
        return;
    }
    snprintf(outfile_buf, n + 1, outfile, timestr);

    // produce the dimensions of the image
    size_t yres = sqrt(3 * total_vmem) / 2;
    size_t xres = (total_vmem + (yres - 1)) / yres;

    // create a squarified treemap of the VMAs
    // see https://www.win.tue.nl/~vanwijk/stm.pdf

    // procedure squarify(list of real :: children, list of real :: row, real :: w)
    // begin
    //     real c = head(children);
    //     if worst(row, w) <= worst(row ++ [c], w) then
    //         squarify(tail(children), row ++ [c], w)
    //     else
    //         layoutrow(row);
    //         squarify(children, [], width());
    //     fi
    // end
    //
    // worst(R; w) = max{r in R}(max((w^2 * r) / s^2; s^2 / (w^2 * r)))
    //             = max((w^2 * r_max) / s^2; s^2 / (w^2 * r_min))

    // extract the number of VMAs from the frame
    uint32_t num_vmas = *(uint32_t*)(buffer + 8);

    // index all vma offsets from the frame
    struct vma *vmas = (struct vma*)calloc(num_vmas, sizeof(*vmas));
    if (!vmas) {
        std::cerr << "calloc: " << strerror(errno) << std::endl;
        return;
    }
    
    size_t index = 12;
    for (size_t i = 0; i < num_vmas; ++i) {
        vmas[i].index = index;

        // get vma start and end
        uint64_t start = *(uint64_t*)(buffer + index);
        uint64_t end = *(uint64_t*)(buffer + index + 8);
        index += 16;

        size_t pages = end - start;

        vmas[i].lower = start;
        vmas[i].upper = end;

        // skip over the name
        uint32_t length = *(uint32_t*)(buffer + index);
        index += 4 + length;

        vmas[i].num_pages = pages;

        // skip the words encoding the page bits
        size_t words = (pages * 2 + (32 - 1)) / 32;
        index += words * 4;
    }

    // sort the vma offsets by descending size
    qsort(vmas, num_vmas, sizeof(*vmas), &vma_cmp);

    // TODO: continue here

    // create the png structures
    png_structp png = png_create_write_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
    if (!png) {
        std::cerr << outfile_buf << ": png_write_create_struct: " << strerror(errno) << std::endl;
        return;
    }

    png_set_user_limits(png, 0x7fffffff, 0x7fffffff);

    png_infop png_info = png_create_info_struct(png);
    if (!png_info) {
        std::cerr << outfile_buf << ": png_create_info_struct: " << strerror(errno) << std::endl;
        return;
    }

    FILE *png_fp = fopen(outfile_buf, "wb");
    if (png_fp == NULL) {
        std::cerr << outfile_buf << ": fopen: " << strerror(errno) << std::endl;
        return;
    }
    png_init_io(png, png_fp);
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
        std::cerr << outfile_buf << ": png_malloc: " << strerror(errno) << std::endl;
        return;
    }

    png_set_PLTE(png, png_info, palette, PNG_MAX_PALETTE_LENGTH);
    png_write_info(png, png_info);
    png_set_packing(png);

    // prepare memory for the image data
    unsigned char *pixels = (unsigned char*)calloc(xres * yres * 3, sizeof(*pixels));
    if (!pixels) {
        std::cerr << outfile_buf << ": calloc: " << strerror(errno) << std::endl;
        return;
    }

    index = 12;
    for (size_t i = 0; i < num_vmas; ++i) {
        uint64_t start = *(uint64_t*)(buffer + index);
        uint64_t end = *(uint64_t*)(buffer + index + 8);
        index += 16;

        size_t pages = end - start;

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

        // skip over the name
        uint32_t length = *(uint32_t*)(buffer + index);
        index += 4 + length;

        size_t words = (pages * 2 + (32 - 1)) / 32;

        uint32_t *page_buffer = (uint32_t*)(buffer + index);

        for (size_t j = 0; j < pages; ++j) {
            int value = (page_buffer[j / 16] >> ((j % 16) * 2)) & 0x3;

            size_t pixel = pixel_offset + j;
            if (pixel >= xres * yres) {
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

    png_bytepp rows = (png_bytepp)png_malloc(png, yres * sizeof(png_bytep));
    if (!rows) {
        std::cerr << outfile_buf << ": calloc: " << strerror(errno) << std::endl;
        return;
    }

    for (size_t i = 0; i < yres; ++i)
        rows[yres - i - 1] = (pixels + (yres - 1 - i) * xres * 3);

    png_write_image(png, rows);
    png_write_end(png, png_info);

    png_free(png, palette);
    png_destroy_write_struct(&png, &png_info);
    fclose(png_fp);
    free(pixels);
    free(outfile_buf);
}
