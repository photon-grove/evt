package evt

// Merge combines two TransactionGroups into one, gracefully handling nil values.
func Merge(group, with TransactionGroup) (TransactionGroup, error) {
	if group == nil {
		return with, nil
	}
	if with == nil {
		return group, nil
	}

	return group.Merge(with)
}
