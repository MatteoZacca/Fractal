package worker

import (
	"testing"
)

func TestChunkStore_Operations(t *testing.T) {
	// Setup an isolated temporary directory for disk I/O
	tempDir := t.TempDir()
	chunkStore := NewChunkStore(tempDir)
	chunkID := "test-chunk-123"
	testData := []byte("shaka fractal")

	t.Run("Create and Stat Chunk", func(t *testing.T) {
		// 1. Create
		file, err := chunkStore.CreateChunk(chunkID)
		if err != nil {
			t.Fatalf("failed to create chunk: %v", err)
		}

		// 2. Write
		_, err = file.Write(testData)
		if err != nil {
			t.Fatalf("failed to write to chunk: %v", err)
		}
		file.Close()

		// 3. Stat
		size, exists, err := chunkStore.RequestChunkSize(chunkID)
		if err != nil {
			t.Fatalf("unexpected error requesting chunk size: %v", err)
		}
		if !exists {
			t.Errorf("expected chunk to exist")
		}
		if size != int64(len(testData)) {
			t.Errorf("expected size %d, got %d", len(testData), size)
		}
	})

	t.Run("Open Existing Chunk", func(t *testing.T) {
		file, err := chunkStore.OpenChunk(chunkID)
		if err != nil {
			t.Fatalf("failed to open chunk: %v", err)
		}
		defer file.Close()

		stat, _ := file.Stat()
		if stat.Size() != int64(len(testData)) {
			t.Errorf("expected opened file to have size %d, got %d", len(testData), stat.Size())
		}
	})

	t.Run("Delete Chunk (Existing and Non-Existing)", func(t *testing.T) {
		// 1. Delete the existing chunk
		err := chunkStore.DeleteChunk(chunkID)
		if err != nil {
			t.Fatalf("failed to delete chunk: %v", err)
		}

		// 2. Verify it is gone
		_, exists, _ := chunkStore.RequestChunkSize(chunkID)
		if exists {
			t.Errorf("expected chunk to be deleted, but it still exists")
		}

		// 3. Delete it again (should not error due to os.IsNotExist handling)
		err = chunkStore.DeleteChunk(chunkID)
		if err != nil {
			t.Errorf("DeleteChunk should ignore missing files, but got error: %v", err)
		}
	})

	t.Run("Stat Missing Chunk", func(t *testing.T) {
		size, exists, err := chunkStore.RequestChunkSize("ghost-chunk")
		if err != nil {
			t.Fatalf("RequestChunkSize should not error on missing files, got: %v", err)
		}
		if exists {
			t.Errorf("expected exists to be false for ghost chunk")
		}
		if size != 0 {
			t.Errorf("expected size 0 for ghost chunk, got %d", size)
		}
	})
}
