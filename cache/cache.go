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
	mu          = sync.RWMutex{}
	cache       map[string]string
	cachename   string
	initialized = false
)

// Set caches a value in the map and writes it to disk
func Set(key string, value string) error {
	mu.Lock()
	defer mu.Unlock()

	if !initialized {
		err := Load(defaultCache)
		if err != nil {
			log.Printf("Looks like the default cache doesn't exist: %s", err)
		}
	}

	append(key, value)

	if _, ok := cache[key]; !ok {
		cache[key] = value
	}

	return nil
}

// Get returns a value from the map
func Get(key string) (string, error) {
	mu.RLock()
	defer mu.RUnlock()

	if !initialized {
		err := Load(defaultCache)
		if err != nil {
			log.Printf("Looks like the default cache doesn't exist: %s", err)
		}
	}

	if v, ok := cache[key]; ok {
		return v, nil
	}

	return "", fmt.Errorf("Key not found")
}

// Load initializes the in-memory map with the information from the disk cache
func Load(filename string) error {
	mu.Lock()
	defer mu.Unlock()

	cachename = filename
	cache = make(map[string]string)
	initialized = true

	// Check if file exists
	if _, err := os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			os.Create(filename)
			log.Printf("Cache file not found, created file '%s'", filename)
		} else {
			return err
		}
	}

	// Open file
	f, err := os.Open(filename)
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
			if _, ok := cache[val[0]]; !ok {
				cache[val[0]] = val[1]
			}
		}
	}

	return nil
}

// Export writes a new disk cache file
func Export(filename string) error {
	mu.Lock()
	defer mu.Unlock()

	cachename = filename
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// Initialize map if it hasn't been
	if !initialized {
		cache = make(map[string]string)
		initialized = true
	}

	for k, v := range cache {
		_, err = fmt.Fprintf(f, "%s_%s\n", k, v)
	}

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
