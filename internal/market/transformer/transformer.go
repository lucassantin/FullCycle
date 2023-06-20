package transformer

import (
	"github.com/lucassantin/FullCycle.git/internal/market/dto"
	"github.com/lucassantin/FullCycle.git/internal/market/entity"
)

func TransformInput(input dto.TradeInput) *entity.Order {
	asset := entity.NewAsset(input.AssetId, input.AssetId, 1000)
	investor := entity.NewInvestor(input.InvestorId)
	order := entity.NewOrder(input.OrderId, investor, asset, input.Shares, input.Price, input.OrderType)
	if input.CurrentShares > 0 {
		assetPosition := entity.NewInvestorAssetPosition(input.AssetId, input.CurrentShares)
		investor.AddAssetPosition(assetPosition)
	}
	reutrn order
}