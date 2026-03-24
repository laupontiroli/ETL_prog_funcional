open FSharp.Data
open ETL.Types



[<EntryPoint>]
let main argv =
    
    // Check if '-e' is present in the arguments; if so, use URLs, otherwise use DataIn folder
    let useExternal = argv |> Array.exists (fun arg -> arg = "-e")

    let writeDBOutput = argv |> Array.exists (fun arg -> arg = "-d")

    let OriginFilter = argv |> Array.exists (fun arg -> arg = "-o")
    let StatusFilter = argv |> Array.exists (fun arg -> arg = "-s")


    let orders, orderItems =
        if useExternal then
            let ordersURL = "https://raw.githubusercontent.com/laupontiroli/ETL_prog_funcional/main/DataIn/order.csv"
            let orderItemsURL = "https://raw.githubusercontent.com/laupontiroli/ETL_prog_funcional/main/DataIn/order_item.csv"
            ETL.ExternalFunctions.Input.loadOrders ordersURL,
            ETL.ExternalFunctions.Input.loadOrderItems orderItemsURL
        else
            let dataInCSV = System.IO.Path.Combine(__SOURCE_DIRECTORY__, "..", "DataIn")
            ETL.ExternalFunctions.Input.loadOrders (System.IO.Path.Combine(dataInCSV, "order.csv")),
            ETL.ExternalFunctions.Input.loadOrderItems (System.IO.Path.Combine(dataInCSV, "order_item.csv"))

    let ordersMap = orders |> Seq.map (fun order -> order.id, order) |> Map.ofSeq  // ofSeq transforma a sequência (chave, valor) em um map (chave, valor)


    let orderItemsWithOrderInfoInitial =
        ETL.Calculations.joinOrderWithItems ordersMap orderItems

    let orderItemsWithOrderInfoFilteredByOrigin =
        if OriginFilter then
            let i = Array.tryFindIndex ((=) "-o") argv
            match i with
            | Some idx when idx + 1 < argv.Length ->
                let originVal = argv.[idx + 1]
                let originParsed = ETL.Transformations.parseFilterOrigin originVal
                orderItemsWithOrderInfoInitial |> Seq.filter (fun orderItem -> orderItem.origin = originParsed)
            | _ -> orderItemsWithOrderInfoInitial
        else
            orderItemsWithOrderInfoInitial

    let orderItemsWithOrderInfo =
        if StatusFilter then
            let i = Array.tryFindIndex ((=) "-s") argv
            match i with
            | Some idx when idx + 1 < argv.Length ->
                let statusVal = argv.[idx + 1]
                let statusParsed = ETL.Transformations.parseFilterStatus statusVal
                orderItemsWithOrderInfoFilteredByOrigin |> Seq.filter (fun orderItem -> orderItem.status = statusParsed) |> Seq.toList
            | _ -> orderItemsWithOrderInfoFilteredByOrigin |> Seq.toList
        else
            orderItemsWithOrderInfoFilteredByOrigin |> Seq.toList


    let totalAmountAndTaxByOrderId =
        ETL.Calculations.calculatetotalAmountAndTaxByOrderId orderItemsWithOrderInfo

    let averageTaxAndAmountByMonthAndYear =
        ETL.Calculations.calculateAverageTaxAndAmountByMonthAndYear orderItemsWithOrderInfo

    if writeDBOutput then
        ETL.ExternalFunctions.Output.WriteDBOutput totalAmountAndTaxByOrderId
    else
        ETL.ExternalFunctions.Output.WriteCSVOutput totalAmountAndTaxByOrderId (System.IO.Path.Combine(__SOURCE_DIRECTORY__, "..", "DataOut", "PatternOutput.csv"))
        ETL.ExternalFunctions.Output.WriteCSVOutput averageTaxAndAmountByMonthAndYear (System.IO.Path.Combine(__SOURCE_DIRECTORY__, "..", "DataOut", "AverageOutput.csv"))  
    0
