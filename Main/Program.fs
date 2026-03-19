open FSharp.Data
open ETL.Types



[<EntryPoint>]
let main argv =
    let dataIn = System.IO.Path.Combine(__SOURCE_DIRECTORY__, "..", "DataIn")

    let orders =
        CsvFile.Load(System.IO.Path.Combine(dataIn, "order.csv")).Rows
        |> Seq.map ETL.Transformations.rowToOrder
        |> Seq.toList

    let orderItems =
        CsvFile.Load(System.IO.Path.Combine(dataIn, "order_item.csv")).Rows
        |> Seq.map ETL.Transformations.rowToOrderItem
        |> Seq.toList

    let ordersMap = orders |> Seq.map (fun order -> order.id, order) |> Map.ofSeq  // ofSeq transforma a sequência (chave, valor) em um map (chave, valor)

    let orderItemsWithOrderInfo =
        ETL.Calculations.joinOrderWithItems ordersMap orderItems

    printfn "Orders: %d" orders.Length
    printfn "First order: %A" orders.Head
    printfn "OrderItems: %d" orderItems.Length
    printfn "First orderItem: %A" orderItems.Head
    0
