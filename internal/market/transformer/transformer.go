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
	return order
}

func TransformOutput(order *entity.Order) *dto.OrderOutput {
	output := dto.OrderOutput{
		OrderId:   order.ID,
		Investor:  order.Investor.ID,
		AssetId:   order.Asset.ID,
		OrderType: order.OrderType,
		Status:    order.Status,
		Partial:   order.PendingShares,
		Shares:    order.Shares,
	}

	var transactionsOutput []*dto.TransactionOutput
	for _, t : range order.Transactions {
		transactionsOutput := &dto.TransactionOutput{
			TransactionID: t.ID,
			BuyerID:       t.BuyingOrder.ID,
			SellerID:      t.SellingOrder.ID,
			AssetID:       t.SellingOrder.Asset.ID,
			Price:         t.Price,
			Shares:        t.SellingOrder.Shares - t.SellingOrder.PendingShares,
		}
		transactionsOutput = append(transactionsOutput, transactionOutput)
	}
	output.TransactionOutput = transactionsOutput
	return output
}