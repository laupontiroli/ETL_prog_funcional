namespace ETL.Types


type Status =
    | Pending
    | Complete
    | Cancelled

type Origin =
    | O
    | P

type Order = {

    id: int
    client_id: int
    order_date: System.DateTime
    status : Status
    origin: Origin
}

type OrderItem = {
    order_id: int
    product_id: int
    quantity: int
    price: float
    tax: float
}

type Output = {
    order_id: int
    total_amount: float
    total_tax: float
} 