module JoinOrderWithItemsTests

open Xunit
open ETL.Types
open ETL.Calculations

let private makeOrder id clientId date status origin : Order =
    { id = id
      client_id = clientId
      order_date = date
      status = status
      origin = origin }

let private makeOrderItem orderId productId qty price tax : OrderItem =
    { order_id = orderId
      product_id = productId
      quantity = qty
      price = price
      tax = tax }

// --- joinOrderWithItems ---

[<Fact>]
let ``joinOrderWithItems maps all fields from order and item correctly`` () =
    let date = System.DateTime(2024, 3, 15)
    let order = makeOrder 1 10 date Complete O
    let orders = Map.ofList [ (1, order) ]
    let items = [ makeOrderItem 1 101 2 10.0 0.1 ]

    let result = joinOrderWithItems orders items

    Assert.Equal(1, result.Length)
    let r = result.[0]
    Assert.Equal(1, r.order_id)
    Assert.Equal(101, r.product_id)
    Assert.Equal(2, r.quantity)
    Assert.Equal(10.0, r.price)
    Assert.Equal(0.1, r.tax)
    Assert.Equal(Complete, r.status)
    Assert.Equal(O, r.origin)
    Assert.Equal(date, r.date)

[<Fact>]
let ``joinOrderWithItems returns empty list for empty items`` () =
    let order = makeOrder 1 10 System.DateTime.Today Pending O
    let orders = Map.ofList [ (1, order) ]

    let result = joinOrderWithItems orders []

    Assert.Empty(result)

[<Fact>]
let ``joinOrderWithItems joins multiple items for the same order`` () =
    let order = makeOrder 5 20 System.DateTime.Today Pending P
    let orders = Map.ofList [ (5, order) ]
    let items = [
        makeOrderItem 5 101 1 10.0 0.1
        makeOrderItem 5 102 2 20.0 0.2
    ]

    let result = joinOrderWithItems orders items

    Assert.Equal(2, result.Length)
    Assert.True(result |> List.forall (fun r -> r.order_id = 5))

[<Fact>]
let ``joinOrderWithItems joins items from multiple different orders`` () =
    let order1 = makeOrder 1 10 System.DateTime.Today Pending O
    let order2 = makeOrder 2 20 System.DateTime.Today Complete P
    let orders = Map.ofList [ (1, order1); (2, order2) ]
    let items = [
        makeOrderItem 1 101 1 10.0 0.1
        makeOrderItem 2 102 2 20.0 0.2
    ]

    let result = joinOrderWithItems orders items

    Assert.Equal(2, result.Length)
    let r1 = result |> List.find (fun r -> r.order_id = 1)
    let r2 = result |> List.find (fun r -> r.order_id = 2)
    Assert.Equal(Pending, r1.status)
    Assert.Equal(O, r1.origin)
    Assert.Equal(Complete, r2.status)
    Assert.Equal(P, r2.origin)

[<Fact>]
let ``joinOrderWithItems propagates Cancelled status`` () =
    let order = makeOrder 7 30 System.DateTime.Today Cancelled O
    let orders = Map.ofList [ (7, order) ]
    let items = [ makeOrderItem 7 200 1 5.0 0.05 ]

    let result = joinOrderWithItems orders items

    Assert.Equal(Cancelled, result.[0].status)

[<Fact>]
let ``joinOrderWithItems throws when item references a missing order_id`` () =
    let orders = Map.ofList [ (1, makeOrder 1 10 System.DateTime.Today Pending O) ]
    let items = [ makeOrderItem 99 101 1 10.0 0.1 ]

    Assert.ThrowsAny<System.Exception>(fun () -> joinOrderWithItems orders items |> ignore) |> ignore

[<Fact>]
let ``joinOrderWithItems result length equals input items length`` () =
    let order = makeOrder 3 15 System.DateTime.Today Complete P
    let orders = Map.ofList [ (3, order) ]
    let items = [
        makeOrderItem 3 101 1 5.0 0.1
        makeOrderItem 3 102 3 8.0 0.2
        makeOrderItem 3 103 2 12.0 0.05
    ]

    let result = joinOrderWithItems orders items

    Assert.Equal(items.Length, result.Length)
