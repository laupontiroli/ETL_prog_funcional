namespace ETL

open FSharp.Data
open ETL.Types

/// Funções de transformação e parsing de dados CSV para os tipos de domínio.
module Transformations =

    /// Converte uma string de argumento CLI no tipo <c>Status</c> para uso como filtro.
    /// Levanta exceção para valores inválidos ou ausentes.
    let parseFilterStatus (str: string) =
        match str with
        | "-s" -> failwith "No value provided for -s option"
        | "P" -> Pending
        | "C" -> Complete
        | "X" -> Cancelled
        | _ -> failwithf "Unknown status: %s" str

    /// Converte uma string de argumento CLI no tipo <c>Origin</c> para uso como filtro.
    /// Levanta exceção para valores inválidos ou ausentes.
    let parseFilterOrigin (str: string) =
        match str with
        | "-o" -> failwith "No value provided for -o option"
        | "O" -> O
        | "P" -> P
        | _ -> failwithf "Unknown origin: %s" str

    /// Converte uma string de status CSV no tipo discriminado <c>Status</c>.
    let parseStatus =
        function
        | "Pending" -> Pending
        | "Complete" -> Complete
        | "Cancelled" -> Cancelled
        | s -> failwithf "Unknown status: %s" s

    /// Converte uma string de origem CSV no tipo discriminado <c>Origin</c>.
    let parseOrigin =
        function
        | "O" -> O
        | "P" -> P
        | s -> failwithf "Unknown origin: %s" s

    /// Transforma uma linha CSV em um registro <c>Order</c>.
    let rowToOrder (row: CsvRow) : Order =
        { id = int row["id"]
          client_id = int row["client_id"]
          order_date = System.DateTime.Parse(row["order_date"])
          status = parseStatus row["status"]
          origin = parseOrigin row["origin"] }

    /// Transforma uma linha CSV em um registro <c>OrderItem</c>.
    let rowToOrderItem (row: CsvRow) : OrderItem =
        { order_id = int row["order_id"]
          product_id = int row["product_id"]
          quantity = int row["quantity"]
          price = float row["price"]
          tax = float row["tax"] }


/// Funções de cálculo e agregação sobre os dados de pedidos.
module Calculations =

    /// Arredonda um valor <c>float</c> para 2 casas decimais.
    let round2 (v: float) = System.Math.Round(v, 2)

    /// Calcula o valor total dos itens de um pedido específico (quantidade × preço).
    let calculateTotalAmount (order_id: int) (orderItemsWithOrderInfo: OrderItemWithOrderInfo seq) : float =
        orderItemsWithOrderInfo
        |> Seq.filter (fun item -> item.order_id = order_id)
        |> Seq.map (fun item -> float item.quantity * item.price)
        |> Seq.fold (+) 0.0
        |> round2

    /// Calcula o imposto total dos itens de um pedido específico (quantidade × taxa × preço).
    let calculateTotalTax (order_id: int) (orderItemsWithOrderInfo: OrderItemWithOrderInfo seq) : float =
        orderItemsWithOrderInfo
        |> Seq.filter (fun item -> item.order_id = order_id)
        |> Seq.map (fun item -> float item.quantity * item.tax * item.price)
        |> Seq.fold (+) 0.0
        |> round2

    /// Realiza um inner join entre pedidos e itens, produzindo uma lista de <c>OrderItemWithOrderInfo</c>.
    let joinOrderWithItems (orders: Map<int, Order>) (orderItems: OrderItem list) : OrderItemWithOrderInfo list =
        orderItems
        |> Seq.map (fun item ->
            let order = orders.[item.order_id]

            { order_id = order.id
              product_id = item.product_id
              quantity = item.quantity
              price = item.price
              tax = item.tax
              status = order.status
              origin = order.origin
              date = order.order_date })
        |> Seq.toList
        
    /// Agrupa os itens por pedido e calcula o total de valor e imposto de cada pedido,
    /// retornando uma lista de <c>Output</c>.
    let calculatetotalAmountAndTaxByOrderId (orderItemsWithOrderInfo: OrderItemWithOrderInfo list) : Output list =
        orderItemsWithOrderInfo
        |> Seq.groupBy (fun item -> item.order_id)
        |> Seq.map (fun (order_id, items) ->
            { order_id = order_id
              total_amount = calculateTotalAmount order_id items
              total_tax = calculateTotalTax order_id items })
        |> Seq.toList

    /// Calcula a média das taxas de uma sequência de itens de pedido.
    let calculateAverageTax (items: OrderItemWithOrderInfo seq) : float =
        items
        |> Seq.map (fun item -> item.tax)
        |> Seq.average
        |> round2

    /// Calcula a média dos preços de uma sequência de itens de pedido.
    let calculateAverageAmount (items: OrderItemWithOrderInfo seq) : float =
        items
        |> Seq.map (fun item -> item.price)
        |> Seq.average
        |> round2

    /// Agrupa os itens por mês e ano e calcula a média de imposto e valor para cada período,
    /// retornando uma lista de <c>AverageTaxAndAmount</c>.
    let calculateAverageTaxAndAmountByMonthAndYear (orderItemsWithOrderInfo: OrderItemWithOrderInfo list) : AverageTaxAndAmount list =
        orderItemsWithOrderInfo
        |> Seq.groupBy (fun item -> item.date.Year, item.date.Month)
        |> Seq.map (fun ((year, month), items) ->
            { month = month
              year = year
              average_tax = calculateAverageTax items
              average_amount = calculateAverageAmount items })
        |> Seq.toList


