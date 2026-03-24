namespace ETL

open FSharp.Data
open ETL.Types

module Transformations =

    let parseFilterStatus (str: string) =
        match str with
        | "-s" -> failwith "No value provided for -s option"
        | "P" -> Pending
        | "C" -> Complete
        | "X" -> Cancelled
        | _ -> failwithf "Unknown status: %s" str

    let parseFilterOrigin (str: string) =
        match str with
        | "-o" -> failwith "No value provided for -o option"
        | "O" -> O
        | "P" -> P
        | _ -> failwithf "Unknown origin: %s" str

    let parseStatus =
        function
        | "Pending" -> Pending
        | "Complete" -> Complete
        | "Cancelled" -> Cancelled
        | s -> failwithf "Unknown status: %s" s

    let parseOrigin =
        function
        | "O" -> O
        | "P" -> P
        | s -> failwithf "Unknown origin: %s" s

    let rowToOrder (row: CsvRow) : Order =
        { id = int row["id"]
          client_id = int row["client_id"]
          order_date = System.DateTime.Parse(row["order_date"])
          status = parseStatus row["status"]
          origin = parseOrigin row["origin"] }

    let rowToOrderItem (row: CsvRow) : OrderItem =
        { order_id = int row["order_id"]
          product_id = int row["product_id"]
          quantity = int row["quantity"]
          price = float row["price"]
          tax = float row["tax"] }


module Calculations =

    let round2 (v: float) = System.Math.Round(v, 2)

    let calculateTotalAmount (order_id: int) (orderItemsWithOrderInfo: OrderItemWithOrderInfo seq) : float =
        orderItemsWithOrderInfo
        |> Seq.filter (fun item -> item.order_id = order_id)
        |> Seq.map (fun item -> float item.quantity * item.price)
        |> Seq.fold (+) 0.0
        |> round2


    let calculateTotalTax (order_id: int) (orderItemsWithOrderInfo: OrderItemWithOrderInfo seq) : float =
        orderItemsWithOrderInfo
        |> Seq.filter (fun item -> item.order_id = order_id)
        |> Seq.map (fun item -> float item.quantity * item.tax * item.price)
        |> Seq.fold (+) 0.0
        |> round2

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
        
    let calculatetotalAmountAndTaxByOrderId (orderItemsWithOrderInfo: OrderItemWithOrderInfo list) : Output list =
        orderItemsWithOrderInfo
        |> Seq.groupBy (fun item -> item.order_id)
        |> Seq.map (fun (order_id, items) ->
            { order_id = order_id
              total_amount = calculateTotalAmount order_id items
              total_tax = calculateTotalTax order_id items })
        |> Seq.toList

    let calculateAverageTax (items: OrderItemWithOrderInfo seq) : float =
        items
        |> Seq.map (fun item -> item.tax)
        |> Seq.average
        |> round2

    let calculateAverageAmount (items: OrderItemWithOrderInfo seq) : float =
        items
        |> Seq.map (fun item -> item.price)
        |> Seq.average
        |> round2

    let calculateAverageTaxAndAmountByMonthAndYear (orderItemsWithOrderInfo: OrderItemWithOrderInfo list) : AverageTaxAndAmount list =
        orderItemsWithOrderInfo
        |> Seq.groupBy (fun item -> item.date.Year, item.date.Month)
        |> Seq.map (fun ((year, month), items) ->
            { month = month
              year = year
              average_tax = calculateAverageTax items
              average_amount = calculateAverageAmount items })
        |> Seq.toList


