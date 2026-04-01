package partitioning

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"
)

// makeHashPartitionRule creates a predicate function that determines whether
// a given workflow ID belongs to the specified partition.
// The partitioning is consistent: the same ID will always map to the same partition.
func makeHashPartitionRule(partitionIndex, totalPartitions int) func(string) bool {
	return func(workflowID string) bool {
		return GetPartitionIndex(workflowID, totalPartitions) == partitionIndex
	}
}

// PartitionedReaderName returns the standard naming convention for a partitioned reader.
// Format: "{workflow_type}_runner_partition_{partition_index}_of_{total_partitions}"
// Example: "order_workflow_runner_partition_0_of_3"
func PartitionedReaderName(workflowType string, partitionIndex, totalPartitions int) string {
	return workflowType + "_runner_partition_" + strconv.Itoa(partitionIndex) + "_of_" + strconv.Itoa(totalPartitions)
}

// GetPartitionIndex calculates the partition index for a given workflow ID.
// It uses MD5 hashing to ensure consistent distribution across partitions.
func GetPartitionIndex(workflowID string, totalPartitions int) int {
	hash := md5.Sum([]byte(workflowID))
	hexStr := hex.EncodeToString(hash[:])
	hashValue, _ := strconv.ParseUint(hexStr, 16, 64)
	return int(hashValue % uint64(totalPartitions))
}

// IsMine returns true if the given workflow ID belongs to the specified partition.
func IsMine(workflowID string, partitionIndex, totalPartitions int) bool {
	return GetPartitionIndex(workflowID, totalPartitions) == partitionIndex
}
