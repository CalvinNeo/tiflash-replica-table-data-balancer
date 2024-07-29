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
			MakeStoreRegionSet(11, []int64{1, 2, 3}),
			MakeStoreRegionSet(12, []int64{4, 5}),
			MakeStoreRegionSet(13, []int64{6}),
		}
		senders, receivers, ops := MigrationPlan(stores)
		require.Equal(t, senders, []int{0})
		require.Equal(t, receivers, []int{2})
		require.Equal(t, len(ValidateMigtationOut(ops, []int64{11})), 1)
	}
	{
		stores := []*StoreRegionSet{
			MakeStoreRegionSet(11, []int64{1, 2, 3, 4, 5}),
			MakeStoreRegionSet(12, []int64{6}),
			MakeStoreRegionSet(13, []int64{7}),
			MakeStoreRegionSet(14, []int64{8}),
			MakeStoreRegionSet(15, []int64{9}),
		}
		senders, receivers, ops := MigrationPlan(stores)
		require.Equal(t, senders, []int{0})
		require.Equal(t, receivers, []int{1, 2, 3})
		require.Equal(t, len(ValidateMigtationOut(ops, []int64{11})), 3)
		require.Equal(t, ValidateMigtationOut(ops, []int64{11}), ValidateMigtationIn(ops, []int64{12, 13, 14, 15}))
	}
	{
		stores := []*StoreRegionSet{
			MakeStoreRegionSet(11, []int64{1, 2, 3, 4, 5}),
			MakeStoreRegionSet(12, []int64{6, 7, 8, 9, 10}),
			MakeStoreRegionSet(13, []int64{11}),
		}
		senders, receivers, ops := MigrationPlan(stores)
		require.Equal(t, senders, []int{0, 1})
		require.Equal(t, receivers, []int{2})
		require.Equal(t, len(ValidateMigtationOut(ops, []int64{11})), 1)
		require.Equal(t, len(ValidateMigtationOut(ops, []int64{12})), 1)
		require.Equal(t, ValidateMigtationOut(ops, []int64{11, 12}), ValidateMigtationIn(ops, []int64{13}))
	}
}
