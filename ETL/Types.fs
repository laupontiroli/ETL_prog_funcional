namespace ETL.Types


/// Representa o status de um pedido.
type Status =
    /// Pedido aguardando processamento.
    | Pending
    /// Pedido concluído com sucesso.
    | Complete
    /// Pedido cancelado.
    | Cancelled

/// Representa a origem de um pedido.
type Origin =
    /// Pedido originado online.
    | O
    /// Pedido originado em loja física.
    | P


/// Representa um pedido realizado por um cliente.
type Order = {
    /// Identificador único do pedido.
    id: int
    /// Identificador do cliente associado ao pedido.
    client_id: int
    /// Data em que o pedido foi realizado.
    order_date: System.DateTime
    /// Status atual do pedido.
    status : Status
    /// Origem do pedido (online ou físico).
    origin: Origin
}

/// Representa um item pertencente a um pedido.
type OrderItem = {
    /// Identificador do pedido ao qual o item pertence.
    order_id: int
    /// Identificador do produto.
    product_id: int
    /// Quantidade do produto no item.
    quantity: int
    /// Preço unitário do produto.
    price: float
    /// Taxa aplicada sobre o produto.
    tax: float
}

/// Representa o resultado agregado de um pedido com total de valor e imposto.
type Output = {
    /// Identificador do pedido.
    order_id: int
    /// Soma total do valor dos itens do pedido.
    total_amount: float
    /// Soma total dos impostos do pedido.
    total_tax: float
} 

/// Representa um item de pedido enriquecido com informações do pedido pai (inner join).
type OrderItemWithOrderInfo = {
    /// Identificador do pedido.
    order_id: int
    /// Identificador do produto.
    product_id: int
    /// Quantidade do produto no item.
    quantity: int
    /// Preço unitário do produto.
    price: float
    /// Taxa aplicada sobre o produto.
    tax: float
    /// Status do pedido.
    status: Status
    /// Origem do pedido.
    origin: Origin
    /// Data do pedido.
    date: System.DateTime
}

/// Representa a média de imposto e valor agrupada por mês e ano.
type AverageTaxAndAmount = {
    /// Média dos impostos no período.
    average_tax: float
    /// Média dos valores no período.
    average_amount: float
    /// Mês de referência.
    month: int
    /// Ano de referência.
    year: int
}
