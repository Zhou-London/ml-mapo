## Data Ingestion Layer

- IPC with other modules.
- Python for demo, C++ when producting.
- Normalize the data into:
```cpp
// An example
struct Asset {
    uint32_t market_idx;
    float O;
    float H;
    float L;
    float C;
    float V;
    uint64_t timestamp;
    // etc.
}

struct Market {
    float regime_idx;
    float benchmark;
    // etc.
}
```