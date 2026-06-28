package weed_server

const (
	volumeRdmaPipelineChunkSize = 4 << 20
	volumeRdmaPipelineDepth     = 16
)

type volumeRdmaChunkRange struct {
	Index  int
	Offset uint64
	Length uint32
}

func planVolumeRdmaChunks(length uint32, chunkSize uint32) []volumeRdmaChunkRange {
	if length == 0 {
		return nil
	}
	if chunkSize == 0 {
		chunkSize = volumeRdmaPipelineChunkSize
	}
	total := int((uint64(length) + uint64(chunkSize) - 1) / uint64(chunkSize))
	chunks := make([]volumeRdmaChunkRange, 0, total)
	for offset := uint64(0); offset < uint64(length); {
		remaining := uint64(length) - offset
		chunkLen := uint64(chunkSize)
		if remaining < chunkLen {
			chunkLen = remaining
		}
		chunks = append(chunks, volumeRdmaChunkRange{
			Index:  len(chunks),
			Offset: offset,
			Length: uint32(chunkLen),
		})
		offset += chunkLen
	}
	return chunks
}
