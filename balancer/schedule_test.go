package balancer

import (
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

func ValidateMigtationOut(ops []*MigrationOp, storeIDs []int64) []int64 {
	r := make(map[int64]interface{})
	for _, op := range ops {
		for _, sid := range storeIDs {
			if op.FromStore == sid {
				for k, _ := range op.Regions {
					r[k] = nil
				}
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

func ValidateMigtationIn(ops []*MigrationOp, storeIDs []int64) []int64 {
	r := make(map[int64]interface{})
	for _, op := range ops {
		for _, sid := range storeIDs {
			if op.ToStore == sid {
				for k, _ := range op.Regions {
					r[k] = nil
				}
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
		require.Equal(t, len(ValidateMigtationOut(ops, []int64{1})), 1)
	}
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
		require.Equal(t, len(ValidateMigtationOut(ops, []int64{1})), 3)
		require.Equal(t, ValidateMigtationOut(ops, []int64{1}), ValidateMigtationIn(ops, []int64{2, 3, 4, 5}))
	}
	{
		stores := []*StoreRegionSet{
			MakeStoreRegionSet(1, []int64{1, 2, 3, 4, 5}),
			MakeStoreRegionSet(2, []int64{6, 7, 8, 9, 10}),
			MakeStoreRegionSet(3, []int64{11}),
		}
		senders, receivers, ops := MigrationPlan(stores)
		require.Equal(t, senders, []int{0, 1})
		require.Equal(t, receivers, []int{2})
		require.Equal(t, len(ValidateMigtationOut(ops, []int64{1})), 1)
		require.Equal(t, len(ValidateMigtationOut(ops, []int64{2})), 1)
		require.Equal(t, ValidateMigtationOut(ops, []int64{1, 2}), ValidateMigtationIn(ops, []int64{3}))
	}
}
