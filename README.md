# GoReed

Simple wrapper for [klauspost](https://github.com/klauspost/reedsolomon) Golang Reedsolomon.

## Description

This program encode files and folders with reedsolomon and splitting them into chunks. It also decode and do verify checksums for files. This is developed with the intention to encode data for tape drive long term archival

## Getting Started

### Dependencies

* Working Go Installation

### Building

```
git clone https://github.com/oversoulmoon/GoReed.git
go build TapeECC.go 
```

### Executing program

```
./TapeECC -m (encode|decode) -p (path to folder or file) -d (number of datashards, default 4, optional) -e (number of parity, default 2, optional)
```

## Authors

[Trung Nguyen](https://github.com/oversoulmoon)

## Acknowledgments

Inspiration, code snippets, etc.
* [go-flags](github.com/jessevdk/go-flags)
* [reedsolomon](github.com/klauspost/reedsolomon)
* [copy](github.com/otiai10/copy)