package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/joho/godotenv"
	"github.com/leonm1/flightsense-go/cache"
	"github.com/leonm1/flightsense-go/flight"
	"github.com/leonm1/flightsense-go/parse"
	csvr "github.com/recursionpharma/go-csv-map"
)

const fileExt = ".csv"

func main() {
	// Send log to stdout and log.txt
	if err := createLog(); err != nil {
		log.Print("Error directing logs to log.txt:", err)
	}

	// Parse command line flags
	in, out, err := parseFlags()
	if err != nil {
		log.Fatal("Could not parse command-line arguments:", err)
	}

	// Load dark sky api key
	if err := populateEnv(); err != nil {
		log.Fatal("Error loading client secrets. Export environment vars or set .env:", err)
	}

	// Load cache from disk. If weather.cache is not present, create an empty cache instead
	c, err := cache.Load("weather.cache")
	if err != nil {
		log.Fatal("Could not initialize cache:", err)
	}

	for _, fn := range in {
		err := processFile(&fn, out, c)
		if err != nil {
			log.Printf("Skipping file %s: ", fn)
		}
	}
}

func processFile(fn, out *string, c *cache.Cache) error {
	var (
		rowCh   = make(chan map[string]string, runtime.GOMAXPROCS(0))
		printCh = make(chan []string)
		wg      = &sync.WaitGroup{}
	)

	// Start worker threads
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go parse.Worker(rowCh, printCh, c, wg)
	}

	// Get output file name
	outFName := *out + filepath.Base(*fn)

	// Start printer thread
	go printer(printCh, &outFName, wg)

	log.Printf("Processing %s to %s...", *fn, outFName)

	// Open file for reading
	f, err := os.Open(*fn)
	if err != nil {
		return fmt.Errorf("could not open %s: %s", *fn, err)
	}

	// Wrap file in csv winter
	r := csvr.NewReader(bufio.NewReader(f))

	h, err := r.ReadHeader()
	if err != nil {
		return fmt.Errorf("could not parse header of %s, is the CSV properly formatted? %s", *fn, err)
	}
	r.Columns = h

	for {
		line, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Printf("Could not parse line in %s: %s", *fn, err)
			continue
		}

		// Send line to workers
		wg.Add(1)
		rowCh <- line
	}

	// Signal end of file
	close(rowCh)

	wg.Wait() // First wait for row handling to end

	wg.Add(1)      // Final wait for end of file flush
	close(printCh) // Signal printer to exit

	wg.Wait() // Second wait for file writing to end

	log.Printf("Finished processing %s.", *fn)

	return nil
}

func populateEnv() error {
	if _, loaded := os.LookupEnv("DARK_SKY_API_KEY"); !loaded {
		if err := godotenv.Load(".env"); err != nil {
			return err
		}
	}

	if _, loaded := os.LookupEnv("DARK_SKY_API_KEY"); !loaded {
		return fmt.Errorf("DARK_SKY_API_KEY not set")
	}

	return nil
}

func createLog() error {
	// Create log file and direct output to file and console
	logFile, err := os.Create("log.txt")
	if err != nil {
		return err
	}
	logW := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(logW)

	return nil
}

func parseFlags() ([]string, *string, error) {
	var (
		in, out string
		recurse bool
		files   []string
	)

	flag.StringVar(&in, "i", "data/", "Input: either a folder or a single csv file")
	flag.StringVar(&out, "o", "out/", "Output: either a folder or a single csv file")
	flag.BoolVar(&recurse, "r", false, "Recurse through subdirectories")
	flag.Parse()

	// Parse wildcard characters
	inGlob, err := filepath.Glob(in)
	if err != nil {
		log.Printf("Error globbing filename: %s", err)
	}

	// Populate list of input files
	for _, v := range inGlob {
		// 'v' is a .csv file
		if filepath.Ext(v) == fileExt {

			// Add file if it exists
			if _, err := os.Stat(in); err == nil {
				files = append(files, v)
			} else { // Error
				return nil, nil, fmt.Errorf("404 - File not found: %s", err)
			}

			// 'v' is a directory
		} else {

			// Search directory for CSV files
			err := filepath.Walk(v, func(p string, inf os.FileInfo, e error) error {

				if e != nil {
					return fmt.Errorf("Error reading directory: %s", e)
				}

				// Skip subdirectory if recursion is disabled
				if !recurse && inf.IsDir() {
					return filepath.SkipDir
				}

				// Add csv files to list of files to process
				if filepath.Ext(p) == fileExt {
					files = append(files, p+inf.Name())
				}

				// No error
				return nil

			})
			// Check error on filepath.Walk
			if err != nil {
				return nil, nil, err
			}
		}
	}

	// Populate output directory
	if filepath.Ext(out) == "" {
		if err := os.MkdirAll(out, 777); err != nil {
			return nil, nil, fmt.Errorf("Could not create output dir")
		}
	} else {
		// Output cannot be a file
		return nil, nil, fmt.Errorf("output must be a directory")
	}

	return files, &out, nil
}

// printer is a worker which uses a buffered writer to write each struct to a csv file
// the function listens on printc for new jobs. Not concurrency safe.
func printer(printCh chan []string, outName *string, wg *sync.WaitGroup) {
	var f flight.Flight

	// Create and open outfile
	outF, err := os.Create(*outName)
	defer outF.Close()
	if err != nil {
		log.Fatalf("Cannot open '%s': %s\n", *outName, err.Error())
	}

	w := csv.NewWriter(outF)
	defer w.Flush()

	// Writer header to file
	w.Write(f.Headers())

	// Pull Flight objects from chan and print to file
	for j := range printCh {
		w.Write(j)
		wg.Done()
	}

	// Last wg addition after loop in processFile()
	wg.Done()
}
