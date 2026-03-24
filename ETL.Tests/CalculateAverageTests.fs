module CalculateAverageTests

open Xunit
open ETL.Types
open ETL.Calculations

let private item price tax : OrderItemWithOrderInfo =
    { order_id = 1
      product_id = 1
      quantity = 1
      price = price
      tax = tax
      status = Pending
      origin = O
      date = System.DateTime(2024, 1, 1) }

// --- calculateAverageTax ---

[<Fact>]
let ``calculateAverageTax returns correct average for multiple items`` () =
    let items = [ item 10.0 0.1; item 20.0 0.2; item 30.0 0.3 ]
    let result = calculateAverageTax (items |> Seq.ofList)
    Assert.Equal(0.2, result, 10)

[<Fact>]
let ``calculateAverageTax returns tax value for single item`` () =
    let items = [ item 10.0 0.15 ]
    let result = calculateAverageTax (items |> Seq.ofList)
    Assert.Equal(0.15, result)

[<Fact>]
let ``calculateAverageTax returns 0 when all taxes are 0`` () =
    let items = [ item 10.0 0.0; item 20.0 0.0; item 30.0 0.0 ]
    let result = calculateAverageTax (items |> Seq.ofList)
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateAverageTax returns 1 when all taxes are 1`` () =
    let items = [ item 10.0 1.0; item 20.0 1.0 ]
    let result = calculateAverageTax (items |> Seq.ofList)
    Assert.Equal(1.0, result)

[<Fact>]
let ``calculateAverageTax handles items with equal tax values`` () =
    let items = [ item 5.0 0.05; item 10.0 0.05; item 15.0 0.05 ]
    let result = calculateAverageTax (items |> Seq.ofList)
    Assert.Equal(0.05, result, 10)

[<Fact>]
let ``calculateAverageTax throws for empty sequence`` () =
    Assert.ThrowsAny<System.Exception>(fun () -> calculateAverageTax (Seq.empty) |> ignore) |> ignore

// --- calculateAverageAmount ---

[<Fact>]
let ``calculateAverageAmount returns correct average for multiple items`` () =
    let items = [ item 10.0 0.1; item 20.0 0.2; item 30.0 0.3 ]
    let result = calculateAverageAmount (items |> Seq.ofList)
    Assert.Equal(20.0, result)

[<Fact>]
let ``calculateAverageAmount returns price for single item`` () =
    let items = [ item 99.99 0.1 ]
    let result = calculateAverageAmount (items |> Seq.ofList)
    Assert.Equal(99.99, result)

[<Fact>]
let ``calculateAverageAmount returns 0 when all prices are 0`` () =
    let items = [ item 0.0 0.1; item 0.0 0.2 ]
    let result = calculateAverageAmount (items |> Seq.ofList)
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateAverageAmount handles items with equal prices`` () =
    let items = [ item 50.0 0.1; item 50.0 0.2; item 50.0 0.3 ]
    let result = calculateAverageAmount (items |> Seq.ofList)
    Assert.Equal(50.0, result)

[<Fact>]
let ``calculateAverageAmount throws for empty sequence`` () =
    Assert.ThrowsAny<System.Exception>(fun () -> calculateAverageAmount (Seq.empty) |> ignore) |> ignore
