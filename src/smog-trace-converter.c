/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#include "./smog-trace-converter.h"

#include <unistd.h>
#include <sys/mman.h>

#include "./args.h"
#include "./tracefile.h"
#include "./backends/parquet.h"
#include "./backends/png.h"
#include "./backends/png-frames.h"

// defaults for cli arguments
struct arguments arguments = { NULL, NULL, 0, OUTPUT_UNKNOWN, 0 };

static const char *output_format_to_string(enum output_format format) {
    switch (format) {
        case OUTPUT_PARQUET:
            return "parquet";
        case OUTPUT_PNG:
            return "png";
        default:
            return "unknown";
    }
}

int main(int argc, char *argv[]) {
    // determine system characteristics
    arguments.page_size = sysconf(_SC_PAGE_SIZE);

    // parse CLI options
    argp_parse(&argp, argc, argv, 0, 0, &arguments);

    printf("SMOG trace converter\n");
    printf("  Loading trace file:     %s\n", arguments.tracefile);
    printf("  Output file:            %s (%s)\n", arguments.output_file,
           output_format_to_string(arguments.output_format));

    struct smog_tracefile tracefile;
    int res = tracefile_open(&tracefile, arguments.tracefile);
    if (res != 0) {
        fprintf(stderr, "%s: ", arguments.tracefile);
        perror("fmmap");
        return 1;
    }

    printf("Indexing frame offsets:   ");
    fflush(stdout);
    res = tracefile_index_frames(&tracefile);
    if (res != 0) {
        perror("error");
        return 1;
    }
    printf("found %zu frames\n", tracefile.num_frames);
    if (arguments.verbose > 1) {
        for (size_t i = 0; i < tracefile.num_frames; ++i) {
            printf("  #%zu: %#zx\n", i, tracefile.frame_offsets[i]);
        }
    }

    switch (arguments.output_format) {
        case OUTPUT_PARQUET:
            res = backend_parquet(&tracefile, arguments.output_file);
            break;
        case OUTPUT_PNG:
            res = backend_png(&tracefile, arguments.output_file);
            break;
        case OUTPUT_PNG_FRAMES:
            res = backend_png_frames(&tracefile, arguments.output_file);
            break;
        default:
            fprintf(stderr, "Encountered unsupported output format. This should not happen.\n");
            return 1;
    }

    if (res != 0) {
        fprintf(stderr, "%s backend failed.\n", output_format_to_string(arguments.output_format));
        return 1;
    }

    // cleanup
    tracefile_close(&tracefile);

    return 0;
}
