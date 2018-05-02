// Package cachemap provides a persistent map-based caching utility for go
package cachemap

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
)

const defaultCache = "cache.txt"

var (
	cache       sync.Map
	cachename   string
	initialized = false
)

// Set caches a value in the map and writes it to disk
func Set(key string, value string) error {
	// Initialize cache
	if !initialized {
		err := Load(defaultCache)
		if err != nil {
			log.Fatalf("Error loading cache: %s", err)
		}
	}

	if _, loaded := cache.LoadOrStore(key, value); !loaded {
		append(key, value)
	}

	return nil
}

// Get returns a value from the map
func Get(key string) (string, error) {
	// Initialize cache
	if !initialized {
		err := Load(defaultCache)
		if err != nil {
			log.Printf("Looks like the default cache doesn't exist: %s", err)
		}
	}

	if v, ok := cache.Load(key); ok {
		return v.(string), nil
	}

	return "", fmt.Errorf("Key not found")
}

// Load initializes the in-memory map with the information from the disk cache
func Load(filename string) error {
	cachename = filename
	initialized = true

	f, err := os.OpenFile(cachename, os.O_CREATE|os.O_RDONLY, 0644)
	defer f.Close()
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(f)

	// Load each line into map
	for scanner.Scan() {
		line := scanner.Text()
		val := strings.Split(line, "_")

		// Load into map
		if len(val) == 2 {
			if _, loaded := cache.LoadOrStore(val[0], val[1]); loaded {
				log.Printf("Duplicate value in cache for key '%s'", val[0])
			}
		}
	}

	return nil
}

// Export writes a new disk cache file
func Export(filename string) error {
	cachename = filename

	f, err := os.OpenFile(cachename, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	if err != nil {
		return err
	}

	cache.Range(func(k interface{}, v interface{}) bool {
		_, err = fmt.Fprintf(f, "%s_%s\n", k.(string), v.(string))
		if err != nil {
			log.Fatalf("Error writing cache file: '%s'", err)
		}

		return true
	})

	return nil
}

func append(k string, v string) error {
	f, err := os.OpenFile(cachename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Print key and value delimited by an underscore '_'
	fmt.Fprintf(f, "%s_%s\n", k, v)

	return err
}
