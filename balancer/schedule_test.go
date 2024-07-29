package balancer

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func MakeStoreRegionSet(id int64, regions []int64) *StoreRegionSet {
	rmap := make(map[int64]bool)
	for _, r := range regions {
		rmap[r] = false
	}
	return &StoreRegionSet{
		ID:          id,
		RegionIDSet: rmap,
	}
}

func MakeMigrationOp(f int64, t int64, regions []int64) *MigrationOp {
	regionsMap := make(map[int64]interface{})
	for _, r := range regions {
		regionsMap[r] = nil
	}
	return &MigrationOp{
		FromStore: f,
		ToStore:   t,
		Regions:   regionsMap,
	}
}

func ValidateMigtationOut(ops []*MigrationOp, storeID int64) []int64 {
	r := make(map[int64]interface{})
	for _, op := range ops {
		if op.FromStore == storeID {
			for k, _ := range op.Regions {
				r[k] = nil
			}
		}
	}
	rl := []int64{}
	for k, _ := range r {
		rl = append(rl, k)
	}
	slices.Sort(rl)
	return rl
}

func TestBalance(t *testing.T) {
	{
		stores := []*StoreRegionSet{
			MakeStoreRegionSet(1, []int64{1, 2, 3}),
			MakeStoreRegionSet(2, []int64{4, 5}),
			MakeStoreRegionSet(3, []int64{6}),
		}
		senders, receivers, ops := MigrationPlan(stores)
		require.Equal(t, senders, []int{0})
		require.Equal(t, receivers, []int{2})
		require.Equal(t, len(ValidateMigtationOut(ops, 1)), 1)
	}
	fmt.Println("=====")
	{
		stores := []*StoreRegionSet{
			MakeStoreRegionSet(1, []int64{1, 2, 3, 4, 5}),
			MakeStoreRegionSet(2, []int64{6}),
			MakeStoreRegionSet(3, []int64{7}),
			MakeStoreRegionSet(4, []int64{8}),
			MakeStoreRegionSet(5, []int64{9}),
		}
		senders, receivers, ops := MigrationPlan(stores)
		require.Equal(t, senders, []int{0})
		require.Equal(t, receivers, []int{1, 2, 3})
		require.Equal(t, len(ValidateMigtationOut(ops, 1)), 3)
	}
}
