/*
MultithreadedCompressor.cpp

A simple multithreaded file compression/decompression tool using zlib.

Build (Linux/macOS):
  g++ -std=c++17 -O2 MultithreadedCompressor.cpp -o mtcompress -lz -pthread

Usage:
  ./mtcompress c input.file output.mtcz 4    # compress with 4 threads
  ./mtcompress d input.mtcz output.file 4    # decompress with 4 threads

Notes:
 - The tool splits the input file into N chunks (N = number of threads or less),
   compresses each chunk in parallel using zlib's compress2(), and writes a
   simple header followed by compressed blocks.
 - Decompression reads the header, then decompresses chunks in parallel and
   writes the reconstructed file.
 - The file format is custom and minimal: magic|version|chunk_count|per-chunk metadata...
 - Requires zlib development headers and library.

Limitations / caveats:
 - This is a demonstration: production-ready tools require more robust format,
   error recovery, streaming strategies, checksums, better memory handling, etc.
*/

#include <zlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <mutex>
#include <string>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <algorithm>

using namespace std;

struct ChunkMeta {
    uint64_t compressed_size;
    uint64_t original_size;
};

// Simple file format header values
const char MAGIC[4] = {'M','T','Z','1'}; // "Multithreaded Zlib v1"
const uint32_t VERSION = 1;

// Helper: get file size
uint64_t file_size(const string &path) {
    ifstream in(path, ios::binary | ios::ate);
    if (!in) return 0;
    return (uint64_t)in.tellg();
}

// Compress a single chunk buffer using zlib
bool compress_chunk(const vector<unsigned char> &inbuf, vector<unsigned char> &outbuf, int level=Z_BEST_SPEED) {
    uLong src_len = (uLong)inbuf.size();
    uLong bound = compressBound(src_len);
    outbuf.resize(bound);
    int ret = compress2(outbuf.data(), &bound, inbuf.data(), src_len, level);
    if (ret != Z_OK) return false;
    outbuf.resize(bound);
    return true;
}

// Decompress a single chunk
bool decompress_chunk(const vector<unsigned char> &inbuf, vector<unsigned char> &outbuf, uint64_t expected_size) {
    uLongf dest_len = (uLongf)expected_size;
    outbuf.resize(dest_len);
    int ret = uncompress(outbuf.data(), &dest_len, inbuf.data(), (uLong)inbuf.size());
    if (ret != Z_OK) return false;
    if (dest_len != expected_size) return false;
    return true;
}

// Write header: MAGIC(4)|VERSION(4)|chunk_count(8)|for each chunk: comp_size(8)|orig_size(8)
void write_header(ofstream &out, const vector<ChunkMeta> &metas) {
    out.write(MAGIC, 4);
    uint32_t ver = VERSION;
    out.write(reinterpret_cast<const char*>(&ver), sizeof(ver));
    uint64_t cnt = metas.size();
    out.write(reinterpret_cast<const char*>(&cnt), sizeof(cnt));
    for (const auto &m : metas) {
        out.write(reinterpret_cast<const char*>(&m.compressed_size), sizeof(m.compressed_size));
        out.write(reinterpret_cast<const char*>(&m.original_size), sizeof(m.original_size));
    }
}

// Read header and metadata
bool read_header(ifstream &in, vector<ChunkMeta> &metas) {
    char magic[4];
    if (!in.read(magic, 4)) return false;
    if (memcmp(magic, MAGIC, 4) != 0) return false;
    uint32_t ver;
    if (!in.read(reinterpret_cast<char*>(&ver), sizeof(ver))) return false;
    if (ver != VERSION) return false;
    uint64_t cnt;
    if (!in.read(reinterpret_cast<char*>(&cnt), sizeof(cnt))) return false;
    metas.resize(cnt);
    for (uint64_t i=0;i<cnt;i++) {
        if (!in.read(reinterpret_cast<char*>(&metas[i].compressed_size), sizeof(metas[i].compressed_size))) return false;
        if (!in.read(reinterpret_cast<char*>(&metas[i].original_size), sizeof(metas[i].original_size))) return false;
    }
    return true;
}

// Compression driver
int compress_file(const string &inpath, const string &outpath, int threads_requested) {
    uint64_t total_size = file_size(inpath);
    if (total_size == 0) {
        cerr << "Empty or missing input file.\n";
        return 1;
    }

    ifstream in(inpath, ios::binary);
    if (!in) { cerr << "Failed to open input file.\n"; return 1; }

    // determine chunk layout
    int nthreads = max(1, threads_requested);
    // limit to at most number of chunks equal to file size (avoid zero-size chunks)
    uint64_t ideal_chunk = (total_size + nthreads - 1) / nthreads;
    // enforce a minimum chunk size (e.g., 64KB) for efficiency
    const uint64_t MIN_CHUNK = 64 * 1024;
    if (ideal_chunk < MIN_CHUNK) {
        ideal_chunk = min<uint64_t>(MIN_CHUNK, total_size);
        nthreads = max<int>(1, (int)((total_size + ideal_chunk - 1) / ideal_chunk));
    }

    vector<vector<unsigned char>> chunks(nthreads);
    vector<uint64_t> orig_sizes(nthreads, 0);

    // read chunks
    for (int i=0;i<nthreads;i++) {
        uint64_t start = (uint64_t)i * ideal_chunk;
        if (start >= total_size) { chunks.resize(i); orig_sizes.resize(i); break; }
        uint64_t remain = total_size - start;
        uint64_t sz = min<uint64_t>(ideal_chunk, remain);
        orig_sizes[i] = sz;
        chunks[i].resize(sz);
        in.read(reinterpret_cast<char*>(chunks[i].data()), sz);
    }
    in.close();

    size_t chunk_count = chunks.size();
    cout << "Compressing " << inpath << " (" << total_size << " bytes) using " << chunk_count << " chunk(s)\n";

    vector<vector<unsigned char>> compressed(chunk_count);
    vector<ChunkMeta> metas(chunk_count);

    auto t0 = chrono::high_resolution_clock::now();

    // launch threads to compress each chunk
    vector<thread> workers;
    for (size_t i=0;i<chunk_count;i++) {
        workers.emplace_back([i,&chunks,&compressed,&metas]() {
            if (!compress_chunk(chunks[i], compressed[i], Z_BEST_COMPRESSION)) {
                cerr << "Compression failed for chunk " << i << "\n";
                metas[i].compressed_size = 0;
                metas[i].original_size = chunks[i].size();
            } else {
                metas[i].compressed_size = compressed[i].size();
                metas[i].original_size = chunks[i].size();
            }
        });
    }
    for (auto &t : workers) t.join();

    auto t1 = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = t1 - t0;

    // write output file: header + compressed blocks
    ofstream out(outpath, ios::binary | ios::trunc);
    if (!out) { cerr << "Failed to open output file for writing.\n"; return 1; }
    write_header(out, metas);
    for (size_t i=0;i<chunk_count;i++) {
        out.write(reinterpret_cast<const char*>(compressed[i].data()), (streamsize)compressed[i].size());
    }
    out.close();

    uint64_t total_compressed = 0;
    for (auto &m : metas) total_compressed += m.compressed_size;

    cout << "Compression done. Time: " << elapsed.count() << "s\n";
    cout << "Original: " << total_size << " bytes, Compressed: " << total_compressed << " bytes\n";
    cout << "Wrote: " << outpath << "\n";
    return 0;
}

// Decompression driver
int decompress_file(const string &inpath, const string &outpath, int threads_requested) {
    ifstream in(inpath, ios::binary);
    if (!in) { cerr << "Cannot open compressed file.\n"; return 1; }

    vector<ChunkMeta> metas;
    if (!read_header(in, metas)) { cerr << "Invalid or corrupted header.\n"; return 1; }
    size_t chunk_count = metas.size();

    cout << "Decompressing using " << chunk_count << " chunk(s)\n";

    vector<vector<unsigned char>> comp_blocks(chunk_count);
    for (size_t i=0;i<chunk_count;i++) {
        comp_blocks[i].resize(metas[i].compressed_size);
        if (!in.read(reinterpret_cast<char*>(comp_blocks[i].data()), (streamsize)metas[i].compressed_size)) {
            cerr << "Failed reading compressed block " << i << "\n"; return 1;
        }
    }
    in.close();

    vector<vector<unsigned char>> decompressed(chunk_count);

    auto t0 = chrono::high_resolution_clock::now();

    vector<thread> workers;
    for (size_t i=0;i<chunk_count;i++) {
        workers.emplace_back([i,&comp_blocks,&decompressed,&metas]() {
            if (!decompress_chunk(comp_blocks[i], decompressed[i], metas[i].original_size)) {
                cerr << "Decompression failed for chunk " << i << "\n";
            }
        });
    }
    for (auto &t : workers) t.join();

    auto t1 = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = t1 - t0;

    // write reassembled file
    ofstream out(outpath, ios::binary | ios::trunc);
    if (!out) { cerr << "Failed to open output file for writing.\n"; return 1; }
    for (size_t i=0;i<chunk_count;i++) {
        out.write(reinterpret_cast<const char*>(decompressed[i].data()), (streamsize)decompressed[i].size());
    }
    out.close();

    cout << "Decompression done. Time: " << elapsed.count() << "s\n";
    cout << "Wrote: " << outpath << "\n";
    return 0;
}

void print_usage() {
    cerr << "Usage:\n  mtcompress c <input> <output.mtcz> <threads>    (compress)\n";
    cerr << "  mtcompress d <input.mtcz> <output> <threads>    (decompress)\n";
}

int main(int argc, char **argv) {
    if (argc < 5) { print_usage(); return 1; }
    string mode = argv[1];
    string in = argv[2];
    string out = argv[3];
    int threads = stoi(argv[4]);
    if (threads <= 0) threads = 1;

    if (mode == "c") {
        auto t0 = chrono::high_resolution_clock::now();
        int res = compress_file(in, out, threads);
        auto t1 = chrono::high_resolution_clock::now();
        chrono::duration<double> tot = t1 - t0;
        cout << "Total elapsed (including I/O): " << tot.count() << "s\n";
        return res;
    } else if (mode == "d") {
        auto t0 = chrono::high_resolution_clock::now();
        int res = decompress_file(in, out, threads);
        auto t1 = chrono::high_resolution_clock::now();
        chrono::duration<double> tot = t1 - t0;
        cout << "Total elapsed (including I/O): " << tot.count() << "s\n";
        return res;
    } else {
        print_usage();
        return 1;
    }
}
