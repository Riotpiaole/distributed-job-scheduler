package datasource

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

type DataSource interface {
	Stream(ctx context.Context) <-chan string
	StreamBytes(ctx context.Context) <-chan []byte
}

var _ DataSource = (*FilesDataSource)(nil)

type FilesDataSource struct {
	FilePath string
	Files    []string
}

// Stream implements DataSource.
func (fd *FilesDataSource) Stream(ctx context.Context) <-chan string {
	terminateCtx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM)

	jobs := make(chan string)

	defer stop()
	defer close(jobs)

	// monitor for shutdown signal
	go func() {
		<-terminateCtx.Done()
		fmt.Println("\n Shutdown signal receieved closed the filestream channel")
		close(jobs)
	}()

	go func() {
		fmt.Println("Waiting for a coordinator to listen...")
		jobs <- "Started"
		err := filepath.WalkDir(fd.FilePath, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err // Stop and return error if a directory can't be accessed
			}

			// Check if it's a file (not a directory)
			if !d.IsDir() {
				jobs <- path
			}

			return nil
		})
		fmt.Printf("failed to go through directory %s\n", err)
	}()
	// WalkDir traverses the file tree rooted at root

	return jobs
}

// StreamBytes implements DataSource.
func (f *FilesDataSource) StreamBytes(ctx context.Context) <-chan []byte {
	panic("unimplemented")
}
