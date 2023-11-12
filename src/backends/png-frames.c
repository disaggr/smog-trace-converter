/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#include "backends/png-frames.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

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

static void write_frame(const char *outfile, struct range *ranges, size_t num_ranges,
                        size_t total_vmem, char *buffer);

int backend_png_frames(struct smog_tracefile *tracefile, const char *path) {
    // check the outfile pattern
    if (!strstr(path, "%s")) {
        fprintf(stderr, "error: OUTFILE must contain '%%s'\n");
        return 1;
    }

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

            // skip over the name
            uint32_t length = *(uint32_t*)(tracefile->buffer + index);
            index += 4 + length;

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

    printf("Writing output frames:    0%%");
    fflush(stdout);

    size_t total_work = tracefile->num_frames;
    size_t work_done = 0;

    #pragma omp parallel for
    for (size_t i = 0; i < tracefile->num_frames; ++i) {
        write_frame(path, ranges, num_ranges, total_vmem,
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

    // cleanup
    free(ranges);

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

static void write_frame(const char *outfile, struct range *ranges, size_t num_ranges,
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
        fprintf(stderr, "failed to create output filename\n");
        return;
    }
    snprintf(timestr + len, 8, ".%06u", usec);

    // produce a path to the output file
    int n = snprintf(NULL, 0, outfile, timestr);
    char *outfile_buf = malloc(n);
    if (outfile_buf == NULL) {
        perror("malloc");
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
    struct vma *vmas = calloc(sizeof(*vmas), num_vmas);
    if (!vmas) {
        perror("calloc");
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
        fprintf(stderr, "%s: ", outfile_buf);
        perror("png_write_create_struct");
        return;
    }

    png_set_user_limits(png, 0x7fffffff, 0x7fffffff);

    png_infop png_info = png_create_info_struct(png);
    if (!png_info) {
        fprintf(stderr, "%s: ", outfile_buf);
        perror("png_create_info_struct");
        return;
    }

    FILE *png_fp = fopen(outfile_buf, "wb");
    if (png_fp == NULL) {
        fprintf(stderr, "%s: ", outfile_buf);
        perror("fopen");
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

    png_colorp palette = png_malloc(png, PNG_MAX_PALETTE_LENGTH * sizeof(png_color));
    if (!palette) {
        fprintf(stderr, "%s: ", outfile_buf);
        perror("png_malloc");
        return;
    }

    png_set_PLTE(png, png_info, palette, PNG_MAX_PALETTE_LENGTH);
    png_write_info(png, png_info);
    png_set_packing(png);

    // prepare memory for the image data
    unsigned char *pixels = calloc(xres * yres * 3, sizeof(*pixels));
    if (!pixels) {
        fprintf(stderr, "%s: ", outfile_buf);
        perror("calloc");
        return;
    }

    index = 12;
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
        index += 4 + length;

        if (end != start + pages) {
            fprintf(stderr, "warning: mismatched VMA range\n");
        }

        size_t words = (pages * 2 + (32 - 1)) / 32;

        uint32_t *page_buffer = (uint32_t*)(buffer + index);

        for (size_t j = 0; j < pages; ++j) {
            int value = (page_buffer[j / 16] >> ((j % 16) * 2)) & 0x3;

            size_t pixel = pixel_offset + j;
            if (pixel >= xres * yres) {
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

    png_bytepp rows = png_malloc(png, yres * sizeof(png_bytep));
    if (!rows) {
        fprintf(stderr, "%s: ", outfile_buf);
        perror("calloc");
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
