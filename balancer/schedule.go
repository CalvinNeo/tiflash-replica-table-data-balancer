package balancer

import (
	"cmp"
	"encoding/hex"
	"fmt"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	client "github.com/Lloyd-Pottiger/tiflash-replica-table-data-balancer/pd_client"
)

func Schedule(pd client.PDClient, tableID int64, zone, region string, dryRun, showOnly bool) error {
	tiflashStoreIDs, _, err := pd.GetAllTiFlashStores(zone, region)
	if err != nil {
		return err
	}
	if len(tiflashStoreIDs) < 2 {
		return errors.New("TiFlash stores less than 2")
	}
	log.Info("schedule run", zap.String("zone", zone), zap.String("region", region), zap.Bool("dry-run", dryRun), zap.Bool("show-only", showOnly))
	if dryRun {
		log.Info("Schedule running in dry-run mode, it will only print the operator commands. If you want to send the operators to PD, add --dry-run=false")
	}
	log.Info("TiFlash stores", zap.String("zone", zone), zap.String("region", region), zap.Int("num-store", len(tiflashStoreIDs)), zap.Any("store-ids", tiflashStoreIDs))
	startKey, endKey, err := pd.GetTableKeyRange(tableID)
	if err != nil {
		return err
	}
	log.Info("Key range for table", zap.Int64("table-id", tableID), zap.String("start-key", hex.EncodeToString(startKey)), zap.String("end-key", hex.EncodeToString(endKey)))
	tiflashStores, err := pd.GetStoreRegionSetInGivenRange(tiflashStoreIDs, startKey, endKey)
	if err != nil {
		return err
	}
	if len(tiflashStores) == 0 {
		return errors.New("This table has no TiFlash replica")
	}

	// Show the distribution among all tiflash stores
	totalRegionCount := 0
	for _, store := range tiflashStores {
		totalRegionCount += len(store.RegionIDSet)
	}
	for _, store := range tiflashStores {
		percentage := 100 * float64(len(store.RegionIDSet)) / float64(totalRegionCount)
		log.Info("store region dist",
			zap.Int64("store-id", store.ID),
			zap.Int("num-region", len(store.RegionIDSet)),
			zap.String("percentage", fmt.Sprintf("%.2f%%", percentage)))
	}
	expectedRegionCountPerStore := totalRegionCount / len(tiflashStores)
	log.Info("Total region peer count", zap.Int("total-num-region-peer", totalRegionCount), zap.Int("expect-num-region-per-store", expectedRegionCountPerStore))
	if showOnly {
		// only show the region distribution
		return nil
	}

	// sort TiFlash stores by region count in descending order
	slices.SortStableFunc(tiflashStores, func(lhs, rhs *client.TiFlashStoreRegionSet) int {
		return -cmp.Compare(len(lhs.RegionIDSet), len(rhs.RegionIDSet))
	})
	// balance TiFlash stores
	// TODO: limit the number of transfer peer operators
	log.Info("balance begin")
	for i := 0; i < len(tiflashStores)-1; i++ {
		for j := len(tiflashStores) - 1; j > i; j-- {
			fromStore := tiflashStores[i]
			toStore := tiflashStores[j]
			fromStoreRegionSet := fromStore.RegionIDSet
			toStoreRegionSet := toStore.RegionIDSet
			numRegionsFromBeg, numRegionsToBeg := len(fromStoreRegionSet), len(toStoreRegionSet)
			numOperatorGen := 0
			log.Info("checking transfer peer",
				zap.Int64("from-store", fromStore.ID), zap.Int64("to-store", toStore.ID),
				zap.Int("num-from-regions-beg", numRegionsFromBeg),
				zap.Int("num-to-regions-beg", numRegionsToBeg))
			for regionID := range fromStoreRegionSet {
				if len(fromStoreRegionSet) <= expectedRegionCountPerStore || len(toStoreRegionSet) >= expectedRegionCountPerStore {
					break
				}
				if _, exist := (toStoreRegionSet)[regionID]; exist {
					// If the region is already in the target store, skip it
					continue
				}
				if dryRun {
					log.Info(fmt.Sprintf("operator add transfer-peer %d %d %d", regionID, fromStore.ID, toStore.ID))
				} else {
					log.Info("transfer peer", zap.Int64("region-id", regionID), zap.Int64("from-store", fromStore.ID), zap.Int64("to-store", toStore.ID))
					if err := pd.AddTransferPeerOperator(regionID, fromStore.ID, toStore.ID); err != nil {
						return err
					}
				}
				delete(fromStoreRegionSet, regionID)
				toStoreRegionSet[regionID] = struct{}{}
				numOperatorGen += 1
			}
			numRegionsFromEnd, numRegionsToEnd := len(fromStoreRegionSet), len(toStoreRegionSet)
			log.Info("generate transfer peer",
				zap.Int64("from-store", fromStore.ID), zap.Int64("to-store", toStore.ID),
				zap.Int("num-from-regions-beg", numRegionsFromBeg), zap.Int("num-from-regions-end", numRegionsFromEnd),
				zap.Int("num-to-regions-beg", numRegionsToBeg), zap.Int("num-to-regions-end", numRegionsToEnd),
				zap.Int("total", numOperatorGen))
		}
	}
	log.Info("balance end")
	return nil
}

type StoreRegionSet struct {
	ID          int64
	RegionIDSet map[int64]bool
}

type MigrationOp struct {
	FromStore int64
	ToStore   int64
	Regions   map[int64]interface{}
}

func PickRegions(n int, fromStore *StoreRegionSet, toStore *StoreRegionSet) *MigrationOp {
	o := MigrationOp{
		FromStore: fromStore.ID,
		ToStore:   toStore.ID,
		Regions:   make(map[int64]interface{}),
	}
	for r, removed := range fromStore.RegionIDSet {
		if n == 0 {
			break
		}
		if removed {
			fmt.Printf("Region %v is removed from %v\n", r, fromStore.ID)
			continue
		}
		fmt.Printf("Pick %v from %v to %v\n", r, fromStore.ID, toStore.ID)
		if _, exist := toStore.RegionIDSet[r]; !exist {
			o.Regions[r] = nil
			fromStore.RegionIDSet[r] = true
		}
		n--
	}
	return &o
}

func MigrationPlan(stores []*StoreRegionSet) ([]int, []int, []*MigrationOp) {
	totalRegionCount := 0
	for _, store := range stores {
		totalRegionCount += len(store.RegionIDSet)
	}
	for _, store := range stores {
		percentage := 100 * float64(len(store.RegionIDSet)) / float64(totalRegionCount)
		log.Info("store region dist",
			zap.Int64("store-id", store.ID),
			zap.Int("num-region", len(store.RegionIDSet)),
			zap.String("percentage", fmt.Sprintf("%.2f%%", percentage)))
	}
	avr := totalRegionCount / len(stores)
	remainder := totalRegionCount % len(stores)
	// sort TiFlash stores by region count in descending order
	slices.SortStableFunc(stores, func(lhs, rhs *StoreRegionSet) int {
		return -cmp.Compare(len(lhs.RegionIDSet), len(rhs.RegionIDSet))
	})
	expectedCount := []int{}
	for i := 0; i < remainder; i++ {
		expectedCount = append(expectedCount, avr+1)
	}
	for i := remainder; i < len(stores); i++ {
		expectedCount = append(expectedCount, avr)
	}
	senders := []int{}
	receivers := []int{}
	sendersVolume := []int{}
	receiversVolume := []int{}
	for i, store := range stores {
		if len(store.RegionIDSet) < expectedCount[i] {
			receivers = append(receivers, i)
			receiversVolume = append(receiversVolume, expectedCount[i]-len(store.RegionIDSet))
		}
		if len(store.RegionIDSet) > expectedCount[i] {
			senders = append(senders, i)
			sendersVolume = append(sendersVolume, len(store.RegionIDSet)-expectedCount[i])
		}
	}

	ops := []*MigrationOp{}

	for i, senderIndex := range senders {
		fromStore := stores[senderIndex]
		for {
			if sendersVolume[i] <= 0 {
				break
			}
			for j, receiverIndex := range receivers {
				toStore := stores[receiverIndex]
				if receiversVolume[j] > 0 {
					n := sendersVolume[i]
					if n > receiversVolume[j] {
						n = receiversVolume[j]
					}
					receiversVolume[j] -= n
					sendersVolume[i] -= n
					fmt.Printf("from %v to %v c %v\n", fromStore.ID, toStore.ID, n)
					ops = append(ops, PickRegions(n, fromStore, toStore))
				}
			}
		}
	}

	return senders, receivers, ops
}
