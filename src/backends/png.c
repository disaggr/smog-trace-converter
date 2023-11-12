/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#include "backends/png.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include <png.h>
#include <omp.h>

#include "./util.h"
#include "./smog-trace-converter.h"

struct range {
    size_t lower;
    size_t upper;
};

static int range_intersects(struct range a, struct range b) {
    return (a.lower <= b.upper && b.lower <= a.upper);
}

static void range_extend(struct range *a, struct range b) {
    if (b.lower < a->lower)
        a->lower = b.lower;
    if (b.upper > a->upper)
        a->upper = b.upper;
}

static void write_frame(unsigned char *pixels, struct range *ranges, size_t num_ranges,
                        size_t width, char *buffer);

int backend_png(struct smog_tracefile *tracefile, const char *path) {
    // aggregate address ranges
    printf("Aggregating VMA Ranges:   ");
    fflush(stdout);
    struct range *ranges = NULL;
    size_t num_ranges = 0;

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
                printf("considering range (%#zx, %#zx)\n", vma.lower, vma.upper);
            }

            int matched = 0;
            for (size_t j = 0; j < num_ranges; ++j) {
                if (vma.lower > ranges[j].upper) {
                    // keep going
                    continue;
                }

                if (range_intersects(ranges[j], vma)) {
                    // overlapping, extend match
                    if (arguments.verbose > 3) {
                        printf("  extending (%#zx, %#zx) ", ranges[j].lower, ranges[j].upper);
                    }
                    range_extend(ranges + j, vma);
                    if (arguments.verbose > 3) {
                        printf("-> (%#zx, %#zx)\n", ranges[j].lower, ranges[j].upper);
                    }
                    matched++;
                    continue;
                }

                if (vma.upper < ranges[j].lower) {
                    // beyond. insert if not matched yet
                    if (!matched) {
                        if (arguments.verbose > 3) {
                            printf("  inserting at %zu\n", j);
                        }
                        struct range *new_ranges = realloc(ranges,
                                                            sizeof(*ranges) * (num_ranges + 1));
                        if (!new_ranges) {
                            perror("realloc");
                            free(ranges);
                            return 1;
                        }
                        ranges = new_ranges;
                        num_ranges++;
                        memmove(ranges + j + 1, ranges + j,
                                (num_ranges - j - 1) * sizeof(*ranges));

                        ranges[j] = vma;
                        matched++;
                    }
                    break;
                }
            }
            if (!matched) {
                // completely new, append.
                if (arguments.verbose > 3) {
                    printf("  appending at %zu\n", num_ranges);
                }
                struct range *new_ranges = realloc(ranges,
                                                   sizeof(*ranges) * (num_ranges + 1));
                if (!new_ranges) {
                    perror("realloc");
                    free(ranges);
                    return 1;
                }

                ranges = new_ranges;
                num_ranges++;

                ranges[num_ranges - 1] = vma;
            }

            // merge adjancent or overlapping ranges
            for (size_t j = 1; j < num_ranges; ++j) {
                if (range_intersects(ranges[j - 1], ranges[j])
                        || ranges[j - 1].upper == ranges[j].lower - 1) {
                    if (arguments.verbose > 3) {
                        printf("  merging (%#zx, %#zx), (%#zx, %#zx) ",
                               ranges[j - 1].lower, ranges[j - 1].upper,
                               ranges[j].lower, ranges[j].upper);
                    }
                    range_extend(ranges + j - 1, ranges[j]);
                    if (arguments.verbose > 3) {
                        printf("-> (%#zx, %#zx)\n", ranges[j - 1].lower, ranges[j - 1].upper);
                    }
                    memmove(ranges + j, ranges + j + 1, (num_ranges - j - 1) * sizeof(*ranges));
                    num_ranges--;
                    j--;
                }
            }

            if (arguments.verbose > 3) {
                printf("%zu ranges\n", num_ranges);
                if (arguments.verbose) {
                    for (size_t i = 0; i < num_ranges; ++i) {
                        size_t num_pages = ranges[i].upper - ranges[i].lower + 1;
                        printf("  (%#zx, %#zx) :: %zu Pages, %s\n",
                               ranges[i].lower, ranges[i].upper, num_pages,
                               format_size_string(num_pages * arguments.page_size));
                    }
                }
            }

            // get name length 
            uint32_t length = *(uint32_t*)(tracefile->buffer + index);
            index += 4;

            // advance over the name
            index += length;

            // advance the index over the pages
            size_t words = (pages * 2 + (32 - 1)) / 32;
            index += words * 4;
        }
    }

    size_t total_vmem = 0;
    for (size_t i = 0; i < num_ranges; ++i) {
        total_vmem += ranges[i].upper - ranges[i].lower + 1;
    }

    printf("found %zu ranges with %zu pages, sized %s\n", num_ranges, total_vmem,
           format_size_string(total_vmem * arguments.page_size));
    if (arguments.verbose) {
        for (size_t i = 0; i < num_ranges; ++i) {
            size_t num_pages = ranges[i].upper - ranges[i].lower + 1;
            printf("  (%#zx, %#zx) :: %zu Pages, %s\n",
                   ranges[i].lower, ranges[i].upper, num_pages,
                   format_size_string(num_pages * arguments.page_size));
        }
    }

    // prepare output file
    size_t yres = tracefile->num_frames;
    size_t xres = total_vmem;

    png_structp png = png_create_write_struct(PNG_LIBPNG_VER_STRING, NULL, NULL, NULL);
    if (!png) {
        fprintf(stderr, "%s: ", path);
        perror("png_write_create_struct");
        return 1;
    }

    png_set_user_limits(png, 0x7fffffff, 0x7fffffff);

    png_infop png_info = png_create_info_struct(png);
    if (!png_info) {
        fprintf(stderr, "%s: ", path);
        perror("png_create_info_struct");
        return 1;
    }

    FILE *png_fp = fopen(path, "wb");
    if (png_fp == NULL) {
        fprintf(stderr, "%s: ", path);
        perror("fopen");
        return 1;
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

    png_colorp palette = png_malloc(png, PNG_MAX_PALETTE_LENGTH * sizeof(png_color));
    if (!palette) {
        fprintf(stderr, "%s: ", path);
        perror("png_malloc");
        return 1;
    }

    png_set_PLTE(png, png_info, palette, PNG_MAX_PALETTE_LENGTH);
    png_write_info(png, png_info);
    png_set_packing(png);

    printf("Writing output frames:    0%%");
    fflush(stdout);

    unsigned char *pixels = calloc(xres * yres * 3, sizeof(*pixels));
    if (!pixels) {
        fprintf(stderr, "%s: ", path);
        perror("calloc");
        return 1;
    }

    size_t total_work = tracefile->num_frames;
    size_t work_done = 0;

    #pragma omp parallel for
    for (size_t i = 0; i < tracefile->num_frames; ++i) {
        write_frame(pixels + i * xres * 3, ranges, num_ranges, xres,
                    tracefile->buffer + tracefile->frame_offsets[i]);

        // progress reporting on the last thread
        if (omp_get_thread_num() == omp_get_num_threads() - 1) {
            work_done++;
            printf("\rWriting output frames:    %zu%%",
                   work_done * omp_get_num_threads() * 100 / total_work);
            fflush(stdout);
        }
    }

    printf("\rWriting output frames:    100%%\n");
    printf("Creating output file:     ");
    fflush(stdout);

    png_bytepp rows = png_malloc(png, yres * sizeof(png_bytep));
    if (!rows) {
        fprintf(stderr, "%s: ", path);
        perror("calloc");
        return 1;
    }

    for (size_t i = 0; i < yres; ++i)
        rows[yres - i - 1] = (pixels + (yres - 1 - i) * xres * 3);

    png_write_image(png, rows);
    png_write_end(png, png_info);
    printf("OK\n");

    printf("Successfully created %zux%zu pixel output image.\n", xres, yres);

    // cleanup
    png_free(png, palette);
    png_destroy_write_struct(&png, &png_info);
    fclose(png_fp);
    free(pixels);
    free(ranges);

    return 0;
}

static void write_frame(unsigned char *pixels, struct range *ranges, size_t num_ranges,
                        size_t width, char *buffer) {
    // extract the number of VMAs from the frame
    uint32_t num_vmas = *(uint32_t*)(buffer + 8);

    size_t index = 12;
    for (size_t i = 0; i < num_vmas; ++i) {
        uint64_t start = *(uint64_t*)(buffer + index);
        uint64_t end = *(uint64_t*)(buffer + index + 8);
        index += 16;

        size_t pages = end - start;

        size_t pixel_offset = 0;
        for (size_t j = 0; j < num_ranges; ++j) {
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
        index += 4;
        index += length;

        size_t words = (pages * 2 + (32 - 1)) / 32;

        uint32_t *page_buffer = (uint32_t*)(buffer + index);

        for (size_t j = 0; j < pages; ++j) {
            int value = (page_buffer[j / 16] >> ((j % 16) * 2)) & 0x3;

            size_t pixel = pixel_offset + j;
            if (pixel >= width) {
                fprintf(stderr, "warning: pixel position out of range\n");
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
}
