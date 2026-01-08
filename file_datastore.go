package cc

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	filestore "github.com/usace-cloud-compute/filesapi"
)

const (
	S3ROOT = "root"
)

type FileDataStoreTypes interface {
	filestore.BlockFS | filestore.S3FS
}

type FileDataStoreInterface interface {
	Get(path string, datapath string) (io.ReadCloser, error)
	GetFilestore() filestore.FileStore
	Put(reader io.Reader, path string, destDataPath string) (int, error)
	//Delete(path string)
	GetSession() any
	GetAbsolutePath(path string) string
}

type FileDataStore[T FileDataStoreTypes] struct {
	fs   filestore.FileStore
	root string
}

func (fds *FileDataStore[T]) GetAbsolutePath(path string) string {
	return fmt.Sprintf("%s/%s", fds.root, path)
}

func (fds *FileDataStore[T]) Get(path string, datapath string) (io.ReadCloser, error) {
	// If no filestore implementation is configured, fall back to local filesystem access.
	if fds.fs == nil {
		fp := filepath.Clean(filepath.Join(fds.root, path))
		f, err := os.Open(fp)
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	fsgoi := filestore.GetObjectInput{
		Path: filestore.PathConfig{Path: fds.root + "/" + path},
	}

	return fds.fs.GetObject(fsgoi)
}


func (fds *FileDataStore[T]) GetFilestore() filestore.FileStore {
	return fds.fs
}

func (fds *FileDataStore[T]) Put(reader io.Reader, path string, destDataPath string) (int, error) {
	// If no filestore implementation is configured, fall back to local filesystem write.
	if fds.fs == nil {
		dest := filepath.Clean(filepath.Join(fds.root, path))
		if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			return -1, err
		}
		out, err := os.Create(dest)
		if err != nil {
			return -1, err
		}
		defer out.Close()
		n, err := io.Copy(out, reader)
		return int(n), err
	}

	poi := filestore.PutObjectInput{
		Source: filestore.ObjectSource{
			Reader: reader,
		},
		Dest: filestore.PathConfig{Path: fds.root + "/" + path},
	}
	//@TODO fix the bytes transferred int
	_, err := fds.fs.PutObject(poi)

	return -1, err
}

func (fds *FileDataStore[T]) Delete(path string) error {
	// If no filestore is configured, delete directly from the filesystem.
	if fds.fs == nil {
		return os.Remove(filepath.Clean(filepath.Join(fds.root, path)))
	}
	return fds.Delete(fds.root + "/" + path) //@TODO...for real?  Does this even work?
}

func (fds *FileDataStore[T]) GetSession() any {
	switch v := any(fds.fs).(type) {
	case *filestore.S3FS:
		return v.GetClient()
	case *filestore.BlockFS:
		return nil //block file system does not return a client.  Direct calls are just that...direct to the os
	default:
		return nil
	}
}

func (fds *FileDataStore[T]) Connect(ds DataStore) (any, error) {
	switch ds.StoreType {
	case FSS3:
		awsconfig := BuildS3Config(ds.DsProfile)
		fs, err := filestore.NewFileStore(awsconfig)
		if err != nil {
			return nil, err
		}
		if root, ok := ds.Parameters[S3ROOT]; ok {
			if rootstr, ok := root.(string); ok {
				return &FileDataStore[T]{fs, rootstr}, nil //@TODO why am i returning my original type?
			} else {
				return nil, errors.New("invalid s3 root parameter.  parameter must be a string")
			}
		} else {
			return nil, errors.New("missing s3 root parameter.  cannot create the store")
		}
	case FSB:
		// For a mounted/block file store, create and return a FileDataStore
		// backed by the BlockFS implementation. Use the configured root if present.
		var rootstr string
		if root, ok := ds.Parameters[S3ROOT]; ok {
			if rootstrval, ok := root.(string); ok {
				rootstr = rootstrval
			} else {
				return nil, errors.New("invalid fs root parameter. parameter must be a string")
			}
		} else {
			rootstr = "/"
		}
		// Instantiate a BlockFS implementation (zero value) and return the configured store
		var blockfs filestore.BlockFS
		return &FileDataStore[T]{fs: &blockfs, root: rootstr}, nil
	}

	//unsupported type
	return nil, fmt.Errorf("unsupported filestore connection")

}
