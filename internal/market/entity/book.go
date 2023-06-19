package entity

import (
	"container/heap"
	"sync"
)

type Book struct {
	Order         []*Order
	Transaction   []*Transaction
	OrdersChan    chan *Order
	OrdersChanOut chan *Order
	Wg            *sync.WaitGroup
}

func NewBook(orderChan chan *Order, orderChanOut chan *Order, Wg *sync.WaitGroup) *Book {
	return &Book{
		Order:         []*Order{},
		Transaction:   []*Transaction{},
		OrdersChan:    orderChan,
		OrdersChanOut: orderChanOut,
		Wg:            Wg,
	}
}

func (b *Book) Trade() {
	buyOrders := NewOrderQueue()
	sellOrders := NewOrderQueue()

	heap.Init(buyOrders)
	heap.Init(sellOrders)

	for order := range b.OrdersChan {
		if order.OrderType == "buy" {
			buyOrders.Push(order)
			if sellOrders.Len() > 0 && sellOrders.Orders[0].Price <= order.Price {
				sellOrder := sellOrders.Pop().(*Order)
				if sellOrder.PendingShares > 0 {
					Transaction := NewTransaction(sellOrder, order, order.Shares, float64(sellOrder.Price))
					b.AddTransaction(Transaction, b.Wg)
					sellOrder.Transactions = append(sellOrder.Transactions, Transaction)
					order.Transactions = append(order.Transactions, Transaction)
					b.OrdersChanOut <- sellOrder
					b.OrdersChanOut <- order
					if sellOrder.PendingShares > 0 {
						sellOrders.Push(sellOrder)
					}
				}
			}
		} else if order.OrderType == "sell" {
			sellOrders.Push(order)
			if buyOrders.Len() > 0 && buyOrders.Orders[0].Price >= order.Price {
				buyOrder := buyOrders.Pop().(*Order)
				if buyOrder.PendingShares > 0 {
					Transaction := NewTransaction(order, buyOrder, order.Shares, float64(buyOrder.Price))
					b.AddTransaction(Transaction, b.Wg)
					buyOrder.Transactions = append(buyOrder.Transactions, Transaction)
					order.Transactions = append(order.Transactions, Transaction)
					b.OrdersChanOut <- buyOrder
					b.OrdersChanOut <- order
					if buyOrder.PendingShares > 0 {
						buyOrders.Push(buyOrder)
					}
				}
			}
		}
	}
}

func (b *Book) AddTransaction(transaction *Transaction, Wg *sync.WaitGroup) {
	defer Wg.Done()

	sellingShares := transaction.SellingOrder.PendingShares
	buyingShares := transaction.BuyingOrder.PendingShares

	minShares := sellingShares
	if buyingShares < minShares {
		minShares = buyingShares
	}

	transaction.SellingOrder.Investor.UpdateAssetPosition(transaction.SellingOrder.Asset.ID, -minShares)
	transaction.AddSellOrderPedingShares(-minShares)

	transaction.BuyingOrder.Investor.UpdateAssetPosition(transaction.BuyingOrder.Asset.ID, minShares)
	transaction.AddBuyOrderPedingShares(-minShares)

	transaction.CalculateTotal(transaction.Shares, float64(transaction.BuyingOrder.Price))
	transaction.CloseBuyOrder()
	transaction.CloseSellOrder()
	b.Transaction = append(b.Transaction, transaction)
}
