/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#ifndef SMOG_TRACE_CONVERTER_H_
#define SMOG_TRACE_CONVERTER_H_

#include <stddef.h>

enum output_format {
    OUTPUT_UNKNOWN,
    OUTPUT_PARQUET,
    OUTPUT_PNG,
    OUTPUT_PNG_FRAMES,
};

struct arguments {
    const char *tracefile;
    const char *output_file;
    int verbose;
    enum output_format output_format;
    size_t page_size;
};

extern struct arguments arguments;

#endif  // SMOG_TRACE_CONVERTER_H_
