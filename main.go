package main

import (
	"errors"
	"fmt"
	"sync"
)

type PaymentSystem struct {
	mu           sync.RWMutex // защищает мапу Users и слайс Transactions
	Users        map[string]*User
	Transactions []Transaction
}

func (p *PaymentSystem) AddUser(u *User) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.Users[u.ID]; ok {
		return errors.New("user already exists")
	}
	p.Users[u.ID] = u
	return nil
}

func (p *PaymentSystem) AddTransaction(t Transaction) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Transactions = append(p.Transactions, t)
}

func (p *PaymentSystem) ProcessingTransactions(t Transaction) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if t.FromID == t.ToID {
		return errors.New("cannot transfer to self")
	}

	from, ok := p.Users[t.FromID]
	if !ok {
		return errors.New("sender not found")
	}
	to, ok := p.Users[t.ToID]
	if !ok {
		return errors.New("receiver not found")
	}

	if err := from.Withdraw(int64(t.Amount)); err != nil {
		return err
	}
	if err := to.Deposit(int64(t.Amount)); err != nil {
		return err
	}

	return nil
}

func (p *PaymentSystem) Worker(ch <-chan Transaction, wg *sync.WaitGroup) {
	defer wg.Done()

	for t := range ch {
		if err := p.ProcessingTransactions(t); err != nil {
			fmt.Printf("%s -> %s %.0f: ошибка: %v\n", t.FromID, t.ToID, t.Amount, err)
		}
	}
}

type Transaction struct {
	FromID string
	ToID   string
	Amount float64
}

type User struct {
	mu      sync.Mutex // защищает Balance
	ID      string
	Name    string
	Balance int64
}

func (u *User) Deposit(amount int64) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if amount <= 0 {
		return errors.New("invalid amount")
	}
	u.Balance += amount
	return nil
}

func (u *User) Withdraw(amount int64) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	switch {
	case amount <= 0:
		return errors.New("invalid amount")
	case u.Balance < amount:
		return errors.New("insufficient funds")
	default:
		u.Balance -= amount
		return nil
	}
}

func main() {
	ps := &PaymentSystem{Users: make(map[string]*User)}

	users := []*User{
		{ID: "1", Name: "Женя", Balance: 100000},
		{ID: "2", Name: "Серёга", Balance: 100},
		{ID: "3", Name: "Аня", Balance: 5000},
	}
	for _, u := range users {
		if err := ps.AddUser(u); err != nil {
			fmt.Printf("add user %s: %v\n", u.ID, err)
		}
	}

	// дубль ID — должен быть отклонён
	if err := ps.AddUser(&User{ID: "1", Name: "Двойник"}); err != nil {
		fmt.Println("add user 1:", err)
	}

	ps.AddTransaction(Transaction{FromID: "1", ToID: "2", Amount: 20000}) // ок
	ps.AddTransaction(Transaction{FromID: "2", ToID: "3", Amount: 15000}) // ок, если воркеры обработают её после первой
	ps.AddTransaction(Transaction{FromID: "3", ToID: "1", Amount: 99999}) // insufficient funds
	ps.AddTransaction(Transaction{FromID: "1", ToID: "42", Amount: 100})  // receiver not found
	ps.AddTransaction(Transaction{FromID: "1", ToID: "1", Amount: 100})   // cannot transfer to self
	ps.AddTransaction(Transaction{FromID: "1", ToID: "3", Amount: -500})  // invalid amount

	ch := make(chan Transaction, len(ps.Transactions))

	fmt.Println("\nОбработка транзакций:")
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go ps.Worker(ch, &wg)
	}

	for _, t := range ps.Transactions {
		ch <- t
	}
	close(ch)
	wg.Wait()

	fmt.Println("\nБалансы:")
	// порядок обхода мапы в Go случайный, поэтому идём по слайсу
	for _, u := range users {
		cur := ps.Users[u.ID]
		fmt.Printf("ID: %s | Имя: %s | Баланс: %d коп.\n", cur.ID, cur.Name, cur.Balance)
	}
}
