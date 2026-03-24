module RowToOrderItemTests

open Xunit
open FSharp.Data
open ETL.Transformations

let private makeOrderItemRow orderId productId quantity price tax =
    let csv = CsvFile.Parse($"order_id,product_id,quantity,price,tax\n{orderId},{productId},{quantity},{price},{tax}")
    csv.Rows |> Seq.head

// --- rowToOrderItem ---

[<Fact>]
let ``rowToOrderItem parses a valid row correctly`` () =
    let row = makeOrderItemRow 1 101 3 19.99 0.1
    let result = rowToOrderItem row
    Assert.Equal(1, result.order_id)
    Assert.Equal(101, result.product_id)
    Assert.Equal(3, result.quantity)
    Assert.Equal(19.99, result.price)
    Assert.Equal(0.1, result.tax)

[<Fact>]
let ``rowToOrderItem parses zero quantity correctly`` () =
    let row = makeOrderItemRow 2 200 0 5.0 0.05
    let result = rowToOrderItem row
    Assert.Equal(0, result.quantity)

[<Fact>]
let ``rowToOrderItem parses zero price and tax correctly`` () =
    let row = makeOrderItemRow 3 300 1 0.0 0.0
    let result = rowToOrderItem row
    Assert.Equal(0.0, result.price)
    Assert.Equal(0.0, result.tax)

[<Fact>]
let ``rowToOrderItem throws on non-integer quantity`` () =
    let row = makeOrderItemRow 4 400 "abc" 10.0 0.1
    Assert.ThrowsAny<System.Exception>(fun () -> rowToOrderItem row |> ignore) |> ignore

[<Fact>]
let ``rowToOrderItem throws on non-numeric price`` () =
    let row = makeOrderItemRow 5 500 2 "not-a-number" 0.1
    Assert.ThrowsAny<System.Exception>(fun () -> rowToOrderItem row |> ignore) |> ignore

[<Fact>]
let ``rowToOrderItem throws on non-numeric tax`` () =
    let row = makeOrderItemRow 6 600 2 10.0 "bad"
    Assert.ThrowsAny<System.Exception>(fun () -> rowToOrderItem row |> ignore) |> ignore
