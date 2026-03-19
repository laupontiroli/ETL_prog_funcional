namespace ETL

open FSharp.Data
open ETL.Types

module Transformations =

    let parseStatus = function
        | "Pending"   -> Pending
        | "Complete"  -> Complete
        | "Cancelled" -> Cancelled
        | s           -> failwithf "Unknown status: %s" s

    let parseOrigin = function
        | "O" -> O
        | "P" -> P
        | s   -> failwithf "Unknown origin: %s" s

    let rowToOrder (row: CsvRow) : Order = {
        id         = int    row["id"]
        client_id  = int    row["client_id"]
        order_date = System.DateTime.Parse(row["order_date"])
        status     = parseStatus row["status"]
        origin     = parseOrigin row["origin"]
    }

    let rowToOrderItem (row: CsvRow) : OrderItem = {
        order_id   = int    row["order_id"]
        product_id = int    row["product_id"]
        quantity   = int    row["quantity"]
        price      = float  row["price"]
        tax        = float  row["tax"]
    }


module Calculations =

    let calculateTotalAmount (order_id: int) (orderItems: OrderItem seq) : float =
        orderItems
        |> Seq.filter (fun item -> item.order_id = order_id)
        |> Seq.map (fun item -> float item.quantity * item.price)
        |> Seq.fold (+) 0.0

    
    let calculateTotalTax (order_id: int) (orderItems: OrderItem seq) : float =
        orderItems
        |> Seq.filter (fun item -> item.order_id = order_id)
        |> Seq.map (fun item -> float item.quantity * item.tax * item.price)
        |> Seq.fold (+) 0.0