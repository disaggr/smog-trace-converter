/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#include "backends/histogram.h"

#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <string>
#include <cstdlib>
#include <cstdint>
#include <cassert>

#include "./util.h"
#include "./smog-trace-converter.h"

struct histogram_data {
    size_t committed;
    size_t accessed;
    size_t dirty;
};

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

struct vma {
    off_t index;
    uint64_t lower;
    uint64_t upper;
    size_t num_pages;
};

int backend_histogram(struct smog_tracefile *tracefile, const char *path) {
    // aggregate address ranges
    std::cout << "Aggregating VMA Ranges:   " << std::flush;

    std::map<std::string, std::vector<range>> ranges;

    for (size_t i = 0; i < tracefile->num_frames; ++i) {
        off_t index = tracefile->frame_offsets[i];

        // advance the index over the frame
        // skip timestamp
        index += 8;

        // get number of VMAs
        uint32_t num_vmas = *(uint32_t*)(tracefile->buffer + index);
        index += 4;

        // extend the list of ranges by each named VMA
        for (uint32_t i = 0; i < num_vmas; ++i) {
            // get VMA start / end
            uint64_t vma_start = *(uint64_t*)(tracefile->buffer + index);
            uint64_t vma_end = *(uint64_t*)(tracefile->buffer + index + 8);
            index += 16;

            // get VMA name
            uint32_t length = *(uint32_t*)(tracefile->buffer + index);
            index += 4;

            if (!length) {
                continue;
            }

            std::string name(tracefile->buffer + index, length - 1);
            index += length;

            if (ranges.find(name) == ranges.end()) {
                ranges[name] = std::vector<range>();
            }

            size_t pages = vma_end - vma_start;

            // insert VMA into active ranges
            struct range vma = { vma_start, vma_end - 1 };
            if (arguments.verbose > 3) {
                std::cout << "considering VMA '" << name << "' with range " << vma << std::endl;
            }

            if (vma.lower == 0 && vma.upper == (size_t)-1) {
                continue;
            }

            int matched = 0;
            for (size_t j = 0; j < ranges[name].size(); ++j) {
                if (vma.lower > ranges[name][j].upper) {
                    // keep going
                    continue;
                }

                if (ranges[name][j].intersects(vma)) {
                    // overlapping, extend match
                    if (arguments.verbose > 3) {
                        std::cout << "  extending " << ranges[name][j];
                    }
                    ranges[name][j] |= vma;
                    if (arguments.verbose > 3) {
                        std::cout << " -> " << ranges[name][j] << std::endl;
                    }
                    matched++;
                    continue;
                }

                if (vma.upper < ranges[name][j].lower) {
                    // beyond. insert if not matched yet
                    if (!matched) {
                        if (arguments.verbose > 3) {
                            std::cout << "  inserting at " << j << std::endl;
                        }
                        ranges[name].insert(ranges[name].begin() + j, vma);
                        matched++;
                    }
                    break;
                }
            }
            if (!matched) {
                // completely new, append.
                if (arguments.verbose > 3) {
                    std::cout << "  appending at " << ranges[name].size() << std::endl;
                }
                //std::cout << ranges[name].size() << std::endl;
                fflush(stdout);
                ranges[name].push_back(vma);
            }

            // merge adjancent or overlapping ranges
            for (size_t j = 1; j < ranges[name].size(); ++j) {
                if (ranges[name][j - 1].intersects(ranges[name][j])
                        || ranges[name][j - 1].upper == ranges[name][j].lower - 1) {
                    if (arguments.verbose > 3) {
                        std::cout << "  merging " << ranges[name][j - 1] << ", " << ranges[name][j];
                    }
                    ranges[name][j - 1] |= ranges[name][j];
                    if (arguments.verbose > 3) {
                        std::cout << "-> " << ranges[name][j - 1] << std::endl;
                    }
                    ranges[name].erase(ranges[name].begin() + j);
                    j--;
                }
            }

            if (arguments.verbose > 3) {
                std::cout << ranges[name].size() << " ranges" << std::endl;
                if (arguments.verbose) {
                    for (size_t i = 0; i < ranges[name].size(); ++i) {
                        size_t num_pages = ranges[name][i].upper - ranges[name][i].lower + 1;
                        std::cout << "  " << ranges[name][i] << " :: " << num_pages << " Pages, "
                                  << format_size_string(num_pages * arguments.page_size)
                                  << std::endl;
                    }
                }
            }

            // advance the index over the pages
            size_t words = (pages * 2 + (32 - 1)) / 32;
            index += words * 4;
        }
    }

    size_t total_vmem = 0;
    std::map<std::string, size_t> named_vmem;
    size_t num_ranges = 0;
    for (const auto& named : ranges) {
        num_ranges += named.second.size();
        size_t vmem = 0;
        for (size_t i = 0; i < named.second.size(); ++i) {
            vmem += named.second[i].upper - named.second[i].lower + 1;
        }
        named_vmem[named.first] = vmem;
        total_vmem += vmem;
    }

    std::cout << "found " << ranges.size() << " named VMAs with " << num_ranges << " ranges and " << total_vmem << " pages, sized "
              << format_size_string(total_vmem * arguments.page_size) << std::endl;
    if (arguments.verbose) {
        for (const auto& named : ranges) {
            std::cout << "  " << named.first.c_str() << std::endl;
            for (size_t i = 0; i < named.second.size(); ++i) {
                size_t num_pages = named.second[i].upper - named.second[i].lower + 1;
                std::cout << "    " << named.second[i] << " :: " << num_pages << " Pages, "
                        << format_size_string(num_pages * arguments.page_size)
                        << std::endl;
            }
        }
    }

    std::map<std::string, std::vector<struct histogram_data>> histogram;
    for (const auto& named : named_vmem) {
        histogram[named.first] = std::vector<struct histogram_data>(named.second);
    }

    for (size_t i = 0; i < tracefile->num_frames; ++i) {
        //write_frame(path, ranges, total_vmem, tracefile->buffer + tracefile->frame_offsets[i]);
        char *buffer = tracefile->buffer + tracefile->frame_offsets[i];
        off_t index = 0;

        // advance over timestamp data
        index += 8;

         // extract the number of VMAs from the frame
        uint32_t num_vmas = *(uint32_t*)(buffer + index);
        index += 4;

        // index all vma offsets from the frame
        std::vector<struct vma> vmas(num_vmas);

        for (size_t i = 0; i < num_vmas; ++i) {
            vmas[i].index = index;

            // get vma start and end
            uint64_t start = *(uint64_t*)(buffer + index);
            uint64_t end = *(uint64_t*)(buffer + index + 8);
            index += 16;

            size_t pages = end - start;

            vmas[i].lower = start;
            vmas[i].upper = end;

            vmas[i].num_pages = pages;

            // get the name
            uint32_t length = *(uint32_t*)(buffer + index);
            index += 4;

            if (!length) {
                continue;
            }

            std::string name(buffer + index, length - 1);
            index += length;

            size_t offset = 0;
            for (size_t j = 0; j < ranges[name].size(); ++j) {
                if (start > ranges[name][j].upper) {
                    offset += ranges[name][j].upper - ranges[name][j].lower + 1;
                } else if (start > ranges[name][j].lower) {
                    offset += start - ranges[name][j].lower;
                } else {
                    break;
                }
            }

            // calculate the words encoding the page bits
            size_t words = (pages * 2 + (32 - 1)) / 32;
            uint32_t *page_buffer = (uint32_t*)(buffer + index);

            // calculate histogram data
            for (size_t j = 0; j < pages; ++j) {
                int value = (page_buffer[j / 16] >> ((j % 16) * 2)) & 0x3;

                size_t pos = offset + j;

                // not reserved: black            (0, 0, 0)
                // reserved and not present: blue (0, 0, 1)
                // present and not accessed: cyan (0, 1, 1)
                // accessed and not dirty: green  (0, 1, 0)
                // dirty: red                     (1, 0, 0)

                if (value > 0)
                    histogram[name][pos].committed += 1;
                if (value > 1)
                    histogram[name][pos].accessed += 1;
                if (value > 2)
                    histogram[name][pos].dirty += 1;
            }

            index += words * 4;
        }
    }

    std::ofstream outfile(path);

    for (const auto& named: ranges) {
        outfile << "VMA " << named.first << std::endl;

        size_t offset = 0;

        for (size_t i = 0; i < named.second.size(); ++i) {
            size_t num_pages = named.second[i].upper - named.second[i].lower;

            uintptr_t base = named.second[i].lower;

            for (size_t j = 0; j < num_pages; ++j) {
                outfile << std::hex << "0x" << (base + j) * arguments.page_size << std::dec << " : " 
                        << histogram[named.first][offset + j].committed << "; "
                        << histogram[named.first][offset + j].accessed << "; "
                        << histogram[named.first][offset + j].dirty << std::endl;
            }            

            offset += num_pages;
        }

    }

    return 0;
}