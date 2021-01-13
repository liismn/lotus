package sectorstorage

import (
	"fmt"

	"github.com/filecoin-project/filecoin-ffi/generated"
	"github.com/filecoin-project/go-address"
	commcid "github.com/filecoin-project/go-fil-commcid"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/extern/sector-storage/storiface"
	"github.com/filecoin-project/specs-storage/storage"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"
)

type PieceInfo struct {
	NumBytes uint64
	CommP    []byte
}

type SealPreCommitParam struct {
	CallID       storiface.CallID `json:"CallID"`
	ProofType    uint
	CachePath    string
	SealedPath   string
	UnsealedPath string
	SectorNumber uint64
	ProverID     []byte
	Ticket       abi.SealRandomness
	Pieces       []PieceInfo
	PiecesLen    uint
}

type SealPreCommitResp struct {
	ErrCode int
	CallID  storiface.CallID `json:"CallID"`
	CommR   []byte
	CommD   []byte
}

func (resp *SealPreCommitResp) GetCids() (storage.SectorCids, error) {
	commR, errCommrSize := commcid.ReplicaCommitmentV1ToCID(resp.CommR[:])
	if errCommrSize != nil {
		return storage.SectorCids{}, errCommrSize
	}
	commD, errCommdSize := commcid.DataCommitmentV1ToCID(resp.CommD[:])
	if errCommdSize != nil {
		return storage.SectorCids{}, errCommrSize
	}
	return storage.SectorCids{
		Unsealed: commD,
		Sealed:   commR,
	}, nil
}

type SealCommitParam struct {
	CallID       storiface.CallID `json:"CallID"`
	ProofType    uint
	CommD        []byte
	CommR        []byte
	CachePath    string
	SealedPath   string
	SectorNumber uint64
	ProverID     address.Address
	Ticket       abi.SealRandomness
	Seed         abi.InteractiveSealRandomness
	Pieces       []PieceInfo
	PiecesLen    uint
}

type SealCommitResp struct {
	ErrCode int
	CallID  storiface.CallID `json:"CallID"`
	Proof   []byte
}

type SealPreCommitErrCode int

const (
	SealPreCommitSuccess = SealPreCommitErrCode(0)
	SealPreCommitFailed  = SealPreCommitErrCode(1)
)

type SealCommitErrCode int

const (
	SealCommitSuccess = SealCommitErrCode(0)
	SealCommitFailed  = SealCommitErrCode(1)
)

func (e SealPreCommitErrCode) Err() *storiface.CallError {
	switch e {
	case SealPreCommitFailed:
		return storiface.Err(storiface.ErrorCode(e), errors.New("P12 Failed"))
	default:
		return nil
	}
}

func (e SealCommitErrCode) Err() *storiface.CallError {
	switch e {
	case SealCommitFailed:
		return storiface.Err(storiface.ErrorCode(e), errors.New("C2 Failed"))
	default:
		return nil
	}
}

type SealPreCommitResult struct {
	err    SealPreCommitErrCode
	callID storiface.CallID
	sealed storage.SectorCids
}

func (m *Manager) sendSealPreCommitRequest(sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) error {
	// clean up previous attempts if they exist (sealed, cache)
	// find existed unseal sector path
	ci := storiface.CallID{
		Sector: sector.ID,
		ID:     uuid.New(),
	}
	var sum abi.UnpaddedPieceSize
	for _, piece := range pieces {
		sum += piece.Size.Unpadded()
	}
	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return err
	}
	ussize := abi.PaddedPieceSize(ssize).Unpadded()
	if sum != ussize {
		return xerrors.Errorf("aggregated piece sizes don't match sector size: %d != %d (%d)", sum, ussize, int64(
			ussize-sum))
	}
	// TODO add check
	proofType := generated.FilRegisteredPoStProofStackedDrgWindow32GiBV1
	// generate proverID
	maddr, err := address.NewIDAddress(uint64(sector.ID.Miner))
	if err != nil {
		return errors.Wrap(err, "failed to convert ActorID to prover id ([32]byte) for FFI")
	}
	proverID := To32ByteArray(maddr.Payload()).Inner

	// filPublicPieceInfos, filPublicPieceInfosLen, err := toFilPublicPieceInfos(pieces)
	filPublicPieceInfos, filPublicPieceInfosLen, err := ToFilPublicPieceInfos(pieces)
	if err != nil {
		return err
	}

	request := SealPreCommitParam{
		CallID:       ci,
		ProofType:    uint(proofType),
		CachePath:    "cache",
		SealedPath:   "sealed",
		UnsealedPath: "unsealed",
		SectorNumber: uint64(sector.ID.Number),
		ProverID:     proverID[:],
		Ticket:       ticket, // [32]byte
		Pieces:       filPublicPieceInfos,
		PiecesLen:    filPublicPieceInfosLen,
	}
	fmt.Println(request)
	// req :=
	return nil
}

func ToFilPublicPieceInfos(src []abi.PieceInfo) ([]PieceInfo, uint, error) {
	out := make([]PieceInfo, len(src))

	for idx := range out {
		commP, err := to32ByteCommP(src[idx].PieceCID)
		if err != nil {
			return nil, 0, err
		}

		out[idx] = PieceInfo{
			NumBytes: uint64(src[idx].Size.Unpadded()),
			CommP:    commP.Inner[:],
		}
	}

	return out, uint(len(out)), nil
}

func to32ByteCommP(pieceCID cid.Cid) (generated.Fil32ByteArray, error) {
	commP, err := commcid.CIDToPieceCommitmentV1(pieceCID)
	if err != nil {
		return generated.Fil32ByteArray{}, errors.Wrap(err, "failed to transform sealed CID to CommP")
	}

	return To32ByteArray(commP), nil
}

func To32ByteArray(in []byte) generated.Fil32ByteArray {
	var out generated.Fil32ByteArray
	copy(out.Inner[:], in)
	return out
}

func (m *Manager) sendSealCommitRequest(sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) error {
	// aquireSector
	ci := storiface.CallID{
		Sector: sector.ID,
		ID:     uuid.New(),
	}
	var sum abi.UnpaddedPieceSize
	for _, piece := range pieces {
		sum += piece.Size.Unpadded()
	}
	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return err
	}
	ussize := abi.PaddedPieceSize(ssize).Unpadded()
	if sum != ussize {
		return xerrors.Errorf("aggregated piece sizes don't match sector size: %d != %d (%d)", sum, ussize, int64(
			ussize-sum))
	}
	// TODO add check
	proofType := generated.FilRegisteredPoStProofStackedDrgWindow32GiBV1
	// generate proverID
	maddr, err := address.NewIDAddress(uint64(sector.ID.Miner))
	if err != nil {
		return errors.Wrap(err, "failed to convert ActorID to prover id ([32]byte) for FFI")
	}

	// filPublicPieceInfos, filPublicPieceInfosLen, err := toFilPublicPieceInfos(pieces)
	filPublicPieceInfos, filPublicPieceInfosLen, err := ToFilPublicPieceInfos(pieces)
	if err != nil {
		return err
	}

	commR, err := to32ByteCommR(cids.Sealed)
	if err != nil {
		return err
	}

	commD, err := to32ByteCommD(cids.Unsealed)
	if err != nil {
		return err
	}

	request := SealCommitParam{
		CallID:       ci,
		ProofType:    uint(proofType),
		CommD:        commD.Inner[:],
		CommR:        commR.Inner[:],
		CachePath:    "cache",
		SealedPath:   "sealed",
		SectorNumber: uint64(sector.ID.Number),
		ProverID:     maddr,
		Ticket:       ticket,
		Seed:         seed,
		Pieces:       filPublicPieceInfos,
		PiecesLen:    filPublicPieceInfosLen,
	}
	fmt.Println(request)
	return nil
}

func to32ByteCommR(sealedCID cid.Cid) (generated.Fil32ByteArray, error) {
	commD, err := commcid.CIDToReplicaCommitmentV1(sealedCID)
	if err != nil {
		return generated.Fil32ByteArray{}, errors.Wrap(err, "failed to transform sealed CID to CommR")
	}

	return To32ByteArray(commD), nil
}
func to32ByteCommD(unsealedCID cid.Cid) (generated.Fil32ByteArray, error) {
	commD, err := commcid.CIDToDataCommitmentV1(unsealedCID)
	if err != nil {
		return generated.Fil32ByteArray{}, errors.Wrap(err, "failed to transform sealed CID to CommD")
	}

	return To32ByteArray(commD), nil
}
func (m *Manager) listenSealPreCommitResponse() {
	for {
		select {
		default:
			// UnMashal json
			var result = SealPreCommitResult{}
			m.returnResult(result.callID, result.sealed, result.err.Err())
		}
	}
}

func (m *Manager) listenSealCommitResponse() {
}
