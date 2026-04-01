package partitioning

import (
	"crypto/md5" // #nosec G501 -- MD5 only for stable partition bucketing (Python Fleuve parity), not security.
	"encoding/hex"
	"strconv"
)

// PartitionedReaderName returns the standard naming convention for a partitioned reader.
// Format: "{workflow_type}_runner_partition_{partition_index}_of_{total_partitions}"
// Example: "order_workflow_runner_partition_0_of_3"
func PartitionedReaderName(workflowType string, partitionIndex, totalPartitions int) string {
	return workflowType + "_runner_partition_" + strconv.Itoa(partitionIndex) + "_of_" + strconv.Itoa(totalPartitions)
}

// GetPartitionIndex calculates the partition index for a given workflow ID.
// It uses MD5 hashing to ensure consistent distribution across partitions.
// MD5 is used only as a deterministic mixing function, not for cryptography.
func GetPartitionIndex(workflowID string, totalPartitions int) int {
	if totalPartitions <= 0 {
		return 0
	}
	tp := uint64(totalPartitions)       // #nosec G115 -- totalPartitions validated > 0 above.
	hash := md5.Sum([]byte(workflowID)) // #nosec G401 -- non-cryptographic partition routing; Fleuve Python parity.
	hexStr := hex.EncodeToString(hash[:])
	hashValue, _ := strconv.ParseUint(hexStr, 16, 64)
	mod := hashValue % tp
	/* #nosec G115 -- mod < tp and totalPartitions is a small positive int from callers. */
	return int(mod)
}

// IsMine returns true if the given workflow ID belongs to the specified partition.
func IsMine(workflowID string, partitionIndex, totalPartitions int) bool {
	return GetPartitionIndex(workflowID, totalPartitions) == partitionIndex
}
