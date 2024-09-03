package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jessevdk/go-flags"
	"github.com/klauspost/reedsolomon"
	cp "github.com/otiai10/copy"
)

var ChunkSize int64 = 32 * 1024 * 1024

type EncodedFile struct {
	Path string
	Size int64
	Hash []byte
}

type Part struct {
	PartNum int
	Size    int64
	Files   []EncodedFile
}

type OriginalFile struct {
	Name  string
	Size  int64
	Hash  []byte
	Parts []Part
}
type SpecFile struct {
	AppName      string
	FolderName   string
	Author       string
	Version      string
	DataShards   int
	ParityShards int
	Files        []OriginalFile
}

func main() {
	var Options struct {
		Mode         string `required:"true" short:"m" long:"mode" description:"Decide which mode you want to use" choice:"encode" choice:"decode" choice:"verifypart" choice:"verifyoriginal"`
		Directory    string `required:"true" short:"p" long:"path" description:"Specify Which directory you want to encode/ decode. For encoding, argument can be a file path or directory path. For decoding must be the path of the Part 0 folder "`
		DataShards   int    `short:"d" default:"4" long:"datashard" description:"How many shards will be data"`
		ParityShards int    `short:"e" default:"2" long:"parityshard" description:"How many shards will be parity data"`
	}
	flags.Parse(&Options)

	PathExist, Error := CheckPath(Options.Directory)
	CheckError(Error)
	if PathExist {
		if Options.Mode == "encode" {
			Path, Error := filepath.Abs(Options.Directory)
			CheckError(Error)

			fmt.Println("Encoding " + Path + "...")
			Encode(Options.Directory, Options.DataShards, Options.ParityShards)
		} else if Options.Mode == "decode" {
			Path, Error := filepath.Abs(Options.Directory)
			CheckError(Error)

			fileInfo, Error := os.Stat(Path)
			CheckError(Error)

			if fileInfo.IsDir() {
				Decode(Path)
			} else {
				fmt.Println("Encoding single file are not supported please put it in a folder")
			}

		} else if Options.Mode == "verifypart" {
			VerifyPart(Options.Directory, false)
		}
	}
}

func Decode(Path string) []string {
	fmt.Println("Processing " + Path + "...")

	var result []string

	if strings.HasPrefix(filepath.Base(Path), "Part") {
		fmt.Println("Looking for folders")
		Folders := make(map[string][]string)
		for _, Dir := range GetAllDir(filepath.Dir(Path)) {
			if strings.HasPrefix(filepath.Base(Dir), "Part-") {
				for _, f := range GetAllDir(Dir) {
					Folders[filepath.Base(f)] = append(Folders[filepath.Base(f)], f)
				}
			}
		}
		Options := make([]string, len(Folders))
		j := 0
		for k := range Folders {
			Options[j] = k
			j++
		}
		for i := 0; i < len(Folders); i++ {
			fmt.Printf("  %d %s (%d Parts)\n", i, Options[i], len(Folders[Options[i]]))
		}
		fmt.Print("Please enter a number: ")

		var choice int
		fmt.Scan(&choice)
		DecodeDir(Folders[Options[choice]])
	}
	return result
}
func DecodeDir(PartPaths []string) {
	sort.Slice(PartPaths, func(i int, j int) bool {
		prefix := "Part-"
		iName := filepath.Dir(PartPaths[i])
		jName := filepath.Dir(PartPaths[j])
		iOrder, Error := strconv.Atoi(filepath.Base(iName)[len(prefix):])
		CheckError(Error)

		jOrder, Error := strconv.Atoi(filepath.Base(jName)[len(prefix):])
		CheckError(Error)
		return iOrder < jOrder
	})
	SpecHash := make([]string, 0)
	for _, Path := range PartPaths {
		if _, Error := os.Stat(filepath.Join(Path, "spec.json")); Error == nil {
			SpecHash = append(SpecHash, string(GenerateMD5(filepath.Join(Path, "spec.json"))))
		}
	}
	fmt.Println("Reading Spec")
	CorrectHash := ModeString(SpecHash)
	CorrectIndex := -1
	for Index, Hash := range SpecHash {
		if Hash == CorrectHash {
			CorrectIndex = Index
		}
	}

	//Load Spec File
	SpecByte, Error := os.ReadFile(filepath.Join(PartPaths[CorrectIndex], "spec.json"))
	CheckError(Error)

	var SpecData SpecFile
	json.Unmarshal(SpecByte, &SpecData)

	fmt.Println("Verifying parts")
	for _, Path := range PartPaths {
		f, e := os.Create(filepath.Join(Path, "spec.json"))
		CheckError(e)
		defer f.Close()

		f.Write(SpecByte)

		VerifyPart(Path, true)
	}

	CurrDir, Error := os.Getwd()
	CheckError(Error)

	FinalDir := filepath.Join(CurrDir, "Completed", SpecData.FolderName)

	Error = os.MkdirAll(FinalDir, os.ModePerm)
	CheckError(Error)

	CopyFolderStructure(PartPaths[0], FinalDir)
	tempf, error := os.Create(filepath.Join(FinalDir, "spec.json"))
	CheckError(error)

	defer tempf.Close()

	tempf.Write(SpecByte)
	for _, file := range SpecData.Files {
		WritePath := filepath.Join(FinalDir, file.Name)
		CheckError(Error)

		f, Error := os.OpenFile(WritePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		CheckError(Error)
		defer f.Close()
		if file.Size > 0 {
			for _, chunk := range file.Parts {
				enc, err := reedsolomon.New(SpecData.DataShards, SpecData.ParityShards)
				CheckError(err)

				shards := make([][]byte, SpecData.DataShards+SpecData.ParityShards)

				for i, encfile := range chunk.Files {

					fmt.Println("Opening", encfile.Path)
					shards[i], err = os.ReadFile(filepath.Join(PartPaths[i], encfile.Path))
					if err != nil {
						fmt.Println("Error reading file", err)
						shards[i] = nil
					}

				}

				ok, err := enc.Verify(shards)
				CheckError(err)
				if ok {
					fmt.Println("No reconstruction needed")
				} else {
					fmt.Println("Verification failed. Reconstructing data")
					err = enc.Reconstruct(shards)
					if err != nil {
						fmt.Println("Reconstruct failed -", err)
						os.Exit(1)
					}
					ok, err = enc.Verify(shards)
					if !ok {
						fmt.Println("Verification failed after reconstruction, data likely corrupted.")
						os.Exit(1)
					}
					CheckError(err)
				}
				fmt.Println("Writting to file...")
				var buff bytes.Buffer

				err = enc.Join(&buff, shards, int(chunk.Size))
				f.Write(buff.Bytes())

				CheckError(err)
			}
		}
	}
	VerifyOriginal(FinalDir)
}

func ModeString(testArray []string) (mode string) {
	//	Create a map and populated it with each value in the slice
	//	mapped to the number of times it occurs
	countMap := make(map[string]int)
	for _, value := range testArray {
		countMap[value] += 1
	}

	//	Find the smallest item with greatest number of occurance in
	//	the input slice
	var max int = 0
	for _, key := range testArray {
		freq := countMap[key]
		if freq > max {
			mode = key
			max = freq
		}
	}
	return
}

func GetAllDir(Directory string) []string {
	Directory, Error := filepath.Abs(Directory)
	CheckError(Error)

	var results []string

	entries, err := os.ReadDir(Directory)
	CheckError(err)

	for _, file := range entries {
		if file.IsDir() {
			results = append(results, filepath.Join(Directory, file.Name()))
		}
	}

	return results
}

func VerifyOriginal(Path string) {
	fmt.Println("Reading Spec File")
	File, Error := ioutil.ReadFile(filepath.Join(Path, "spec.json"))
	CheckError(Error)
	var Spec SpecFile
	json.Unmarshal(File, &Spec)

	fmt.Println("Begin Verification")
	var NumOk uint64 = 0
	var NumSkip uint64 = 0
	var NumError uint64 = 0
	var wg sync.WaitGroup
	for _, File := range Spec.Files {
		wg.Add(1)

		go func(File OriginalFile) {
			defer wg.Done()
			fmt.Println(filepath.Join(Path, File.Name))
			if _, Error := os.Stat(filepath.Join(Path, File.Name)); Error == nil {
				Hash := GenerateMD5(filepath.Join(Path, File.Name))
				if bytes.Equal(Hash, File.Hash) {
					fmt.Printf("Verifying %s: OK\n", File.Name)
					atomic.AddUint64(&NumOk, 1)
				} else {
					fmt.Printf("Verifying %s: Failed\n", File.Name)

					atomic.AddUint64(&NumError, 1)
				}
			} else if errors.Is(Error, os.ErrNotExist) {
				atomic.AddUint64(&NumSkip, 1)
			} else {
				atomic.AddUint64(&NumError, 1)
			}
		}(File)
	}
	wg.Wait()
	if NumError == 0 {
		fmt.Println("VERIFICATION PASSED")
	} else {
		fmt.Println("VERIFICATION FAILED")
	}
	fmt.Printf("Done\nFiles Ok: %d\nFiles Errored: %d\nFiles Skipped: %d\n", NumOk, NumError, NumSkip)
}

func VerifyPart(Path string, delete bool) {
	fmt.Println("Reading Spec File")
	File, Error := ioutil.ReadFile(filepath.Join(Path, "spec.json"))
	CheckError(Error)

	var Spec SpecFile
	json.Unmarshal(File, &Spec)

	fmt.Println("Begin Verification")
	var NumOk uint64 = 0
	var NumSkip uint64 = 0
	var NumError uint64 = 0
	var wg sync.WaitGroup
	for _, File := range Spec.Files {
		wg.Add(1)
		go func(File OriginalFile) {
			defer wg.Done()
			for _, Part := range File.Parts {
				for _, EncFile := range Part.Files {
					EncFilePath := filepath.Join(Path, EncFile.Path)
					if _, Error := os.Stat(EncFilePath); Error == nil {
						Hash := GenerateMD5(EncFilePath)
						if bytes.Equal(Hash, EncFile.Hash) {
							fmt.Printf("Verifying %s: OK\n", EncFile.Path)
							atomic.AddUint64(&NumOk, 1)
						} else {
							fmt.Printf("Verifying %s: Failed\n", EncFile.Path)

							atomic.AddUint64(&NumError, 1)
							if delete {
								os.Remove(EncFilePath)
							}
						}

					} else if errors.Is(Error, os.ErrNotExist) {
						atomic.AddUint64(&NumSkip, 1)
					} else {
						atomic.AddUint64(&NumError, 1)
					}
				}
			}
		}(File)
	}
	wg.Wait()
	if NumError == 0 {
		fmt.Println("VERIFICATION PASSED")
	} else {
		fmt.Println("VERIFICATION FAILED")
	}
	fmt.Printf("Done\nFiles Ok: %d\nFiles Errored: %d\nFiles Skipped: %d\n", NumOk, NumError, NumSkip)
}

func CheckPath(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func Encode(Path string, DataShards, ParityShards int) {
	// These will be arguments
	TotalShards := DataShards + ParityShards

	// Convert dir to absolute path
	Directory, Error := filepath.Abs(Path)
	CheckError(Error)

	// Get current folder

	CurrentPath, Error := os.Getwd()
	CheckError(Error)

	PartPaths := make([]string, TotalShards)

	// Create Part Folders
	for i := 0; i < TotalShards; i++ {
		fmt.Println("Creating Part Folder " + strconv.Itoa(i) + "...")
		PartPath := filepath.Join(CurrentPath, ("Part-" + strconv.Itoa(i)))
		PartPaths[i] = PartPath

		Error = os.MkdirAll(PartPath, os.ModePerm)
		CheckError(Error)
	}

	// To check if file is directory or file
	fileinfo, Error := os.Stat(Directory)
	CheckError(Error)

	if fileinfo.IsDir() {
		fmt.Println("Starting preprocessing...")
		ProcessEncodedir(Path, DataShards, ParityShards, PartPaths)
	} else {
		// ProcessFile(Directory, DataShards, ParityShards)
	}
}

func ProcessEncodedir(Directory string, DataShards, ParityShards int, PartPaths []string) {
	Directory, Error := filepath.Abs(Directory)
	CheckError(Error)

	fmt.Println("Resolving symlinks...")
	// To resolve all symlink
	GetAllFilesRecurse(Directory)

	Name := filepath.Base(Directory)
	// TotalShards := DataShards + ParityShards

	// Generate Hash file that contains all MD5 of all files
	fmt.Println("Generating hash for original files...")

	fmt.Println("Copying folder structure to part folders...")
	// Copy over folder structure
	for i := 0; i < len(PartPaths); i++ {
		PartPaths[i] = filepath.Join(PartPaths[i], Name)
		CopyFolderStructure(Directory, PartPaths[i])
	}

	fmt.Println("Begin encoding...")
	EncodeECCDir(Directory, DataShards, ParityShards, PartPaths)
}

func EncodeECCDir(Directory string, DataShards, ParityShards int, PartPaths []string) {

	Directory, Error := filepath.Abs(Directory)
	CheckError(Error)

	// Call recurse to get all files in the directory
	fmt.Println("Getting all files...")
	Files := GetAllFilesRecurse(Directory)

	fmt.Println("Generating Spec Info")
	Spec := SpecFile{AppName: "GoReed", FolderName: filepath.Base(Directory), Author: "Oversoul", Version: "beta-0.1.0", DataShards: DataShards, ParityShards: ParityShards, Files: make([]OriginalFile, len(Files))}

	// var wg sync.WaitGroup
	// Generate Shards for each files
	for i := 0; i < len(Files); i++ {
		// wg.Add(1)
		// go func(ele string, index int) {
		// 	defer wg.Done()
		Spec.Files[i] = EncodeECC(Directory, Files[i], DataShards, ParityShards, PartPaths)
		// }(Files[i], i)
	}
	// wg.Wait()

	fmt.Println("Writting spec files")
	// Write spec file in each directory
	for _, Part := range PartPaths {
		File, Error := os.Create(filepath.Join(Part, "spec.json"))
		CheckError(Error)

		defer File.Close()

		Json, Error := json.MarshalIndent(Spec, "", "  ")
		CheckError(Error)

		File.Write(Json)
	}

}

func EncodeECC(Basepath, Path string, dataShards int, parShards int, PartPaths []string) (result OriginalFile) {

	RelativePath, Error := filepath.Rel(Basepath, Path)
	CheckError(Error)

	RelativePath = filepath.ToSlash(RelativePath)

	fmt.Printf("Encoding %s...\n", RelativePath)

	File, Error := os.Open(Path)
	CheckError(Error)

	defer File.Close()

	FileInfo, Error := File.Stat()
	CheckError(Error)

	FileSize := FileInfo.Size()
	result.Name = RelativePath
	result.Size = FileSize
	result.Hash = GenerateMD5(Path)

	if FileSize == 0 {

		EncPart := Part{PartNum: 0, Files: make([]EncodedFile, 0), Size: 0}

		// If file size if 0, create the files and return only 1 EncodedFile

		EncFile := EncodedFile{Path: RelativePath, Size: FileSize, Hash: GenerateMD5(Path)}
		EncPart.Files = append(EncPart.Files, EncFile)
		result.Parts = append(result.Parts, EncPart)

		for _, Part := range PartPaths {
			TempPath := filepath.Join(Part, RelativePath)

			File, Error := os.Create(TempPath)
			CheckError(Error)

			File.Close()
		}
	} else {
		// Open the input file
		File, Error := os.Open(Path)
		CheckError(Error)
		defer File.Close()

		// Get file info to determine its size
		FileInfo, Error := File.Stat()
		CheckError(Error)

		// Read and split the file into chunks
		Buffer := make([]byte, ChunkSize)
		WaitGroup := sync.WaitGroup{}

		MaxChunks := FileInfo.Size() / ChunkSize
		if FileInfo.Size()%ChunkSize != 0 {
			MaxChunks++
		}

		result.Parts = make([]Part, MaxChunks)

		for ChunkNum := range MaxChunks {
			// Read file
			ReadSize, Error := File.Read(Buffer)
			CheckError(Error)
			WaitGroup.Add(1)

			// Async encode the split parts
			go func(Data []byte, DataSize, Chunk int) {

				EncPart := Part{PartNum: Chunk, Files: make([]EncodedFile, dataShards+parShards), Size: int64(DataSize)}

				// Create encoders and shards
				Encoder, Error := reedsolomon.New(dataShards, parShards)
				CheckError(Error)
				// fmt.Println(Data[:DataSize])
				// fmt.Println(DataSize)

				// Split the Shards
				Shards, Error := Encoder.Split(Data[:DataSize])
				CheckError(Error)
				// Create parity shards
				Error = Encoder.Encode(Shards)
				CheckError(Error)
				for j := 0; j < dataShards+parShards; j++ {

					// Create part files path
					RelCurrPath := fmt.Sprintf("%s.Part-%d.%d", RelativePath, Chunk, j)
					CurrPath := filepath.Join(PartPaths[j], RelCurrPath)

					PartFile, Error := os.Create(CurrPath)
					CheckError(Error)

					PartFile.Write(Shards[j])
					EncFile := EncodedFile{Path: RelCurrPath, Size: int64(len(Shards[j])), Hash: GenerateMD5(CurrPath)}

					EncPart.Files[j] = EncFile
					PartFile.Close()

				}
				result.Parts[ChunkNum] = EncPart
				WaitGroup.Done()

			}(append(Buffer, 0), ReadSize, int(ChunkNum))
		}
		WaitGroup.Wait()
	}
	sort.Slice(result.Parts, func(i, j int) bool {
		return result.Parts[i].PartNum < result.Parts[j].PartNum
	})
	return result
}

func ResolveSymLink(SymLink string) []string {
	ResolvedPath, Error := filepath.EvalSymlinks(SymLink)
	CheckError(Error)

	ResolvedInfo, Error := os.Stat(ResolvedPath)
	CheckError(Error)

	var results []string

	os.Remove(SymLink)

	if ResolvedInfo.IsDir() {
		os.MkdirAll(filepath.ToSlash(SymLink), os.ModePerm)
		cp.Copy(ResolvedPath, SymLink)
		results = append(results, GetAllFilesRecurse(SymLink)...)
	} else {
		cp.Copy(ResolvedPath, SymLink)
		results = append(results, SymLink)
	}

	return results
}

func GetAllFilesRecurse(Directory string) []string {

	Directory, Error := filepath.Abs(Directory)
	CheckError(Error)

	var results []string

	entries, err := os.ReadDir(Directory)
	CheckError(err)

	for _, file := range entries {
		FileInfo, Error := file.Info()
		CheckError(Error)
		if file.IsDir() {
			results = append(results, GetAllFilesRecurse(filepath.Join(Directory, file.Name()))...)
		} else if FileInfo.Mode().Type() == fs.ModeSymlink {
			Path := filepath.Join(Directory, file.Name())
			fmt.Println("FOUND SYMLINK AT " + Path + ". Resolving")
			ResolveSymLink(Path)
		} else {
			results = append(results, filepath.Join(Directory, file.Name()))
		}
	}

	return results
}

func GenerateMD5Dir(Path string) []byte {
	Path, Error := filepath.Abs(Path)
	CheckError(Error)

	Files := GetAllFilesRecurse(Path)
	// Map of file(s) hashes
	Hash := make(map[string][]byte)

	for _, Element := range Files {
		// Get relative path so the map store hash in term of root folder
		RelativePath, Error := filepath.Rel(Path, Element)
		CheckError(Error)

		// Store in map
		Hash[RelativePath] = GenerateMD5(Element)

	}
	// Convert Map to JSON
	JsonString, Error := json.MarshalIndent(Hash, "", "  ")
	CheckError(Error)

	return JsonString
}

func CheckError(E error) {
	if E != nil {
		fmt.Println("Error detected")
		fmt.Println(E)
		os.Exit(2)
	}
}

func CopyFolderStructure(Source, Destionation string) {
	Option := cp.Options{
		Skip: func(srcinfo os.FileInfo, src, dest string) (bool, error) {
			return !srcinfo.IsDir(), nil
		},
	}

	cp.Copy(Source, Destionation, Option)
}

func GenerateMD5(Path string) []byte {
	Path, Error := filepath.Abs(Path)
	CheckError(Error)

	File, Error := os.Open(Path)
	CheckError(Error)
	defer File.Close()

	Hash := md5.New()
	_, err := io.Copy(Hash, File)
	CheckError(err)

	return Hash.Sum(nil)
}
