module CalculateTotalAmountAndTaxByOrderIdTests

open Xunit
open ETL.Types
open ETL.Calculations

let private item orderId productId qty price tax : OrderItemWithOrderInfo =
    { order_id = orderId
      product_id = productId
      quantity = qty
      price = price
      tax = tax
      status = Pending
      origin = O
      date = System.DateTime(2024, 1, 1) }

// --- calculatetotalAmountAndTaxByOrderId ---
// groups all items by order_id and computes total_amount (qty*price) and
// total_tax (qty*tax*price) for each order

[<Fact>]
let ``calculatetotalAmountAndTaxByOrderId returns empty list for empty input`` () =
    let result = calculatetotalAmountAndTaxByOrderId []
    Assert.Empty(result)

[<Fact>]
let ``calculatetotalAmountAndTaxByOrderId returns one output per distinct order_id`` () =
    let items = [
        item 1 101 1 10.0 0.1
        item 2 102 1 20.0 0.2
        item 3 103 1 30.0 0.3
    ]
    let result = calculatetotalAmountAndTaxByOrderId items
    Assert.Equal(3, result.Length)

[<Fact>]
let ``calculatetotalAmountAndTaxByOrderId calculates total_amount correctly`` () =
    let items = [
        item 1 101 2 10.0 0.1   // 2 * 10.0 = 20.0
        item 1 102 3  5.0 0.05  // 3 *  5.0 = 15.0
    ]
    let result = calculatetotalAmountAndTaxByOrderId items
    Assert.Equal(1, result.Length)
    let r = result |> List.find (fun o -> o.order_id = 1)
    Assert.Equal(35.0, r.total_amount)

[<Fact>]
let ``calculatetotalAmountAndTaxByOrderId calculates total_tax correctly`` () =
    let items = [
        item 1 101 2 10.0 0.1   // 2 * 0.1 * 10.0 = 2.0
        item 1 102 3  5.0 0.05  // 3 * 0.05 * 5.0  = 0.75
    ]
    let result = calculatetotalAmountAndTaxByOrderId items
    let r = result |> List.find (fun o -> o.order_id = 1)
    Assert.Equal(2.75, r.total_tax)

[<Fact>]
let ``calculatetotalAmountAndTaxByOrderId handles multiple orders independently`` () =
    let items = [
        item 1 101 1 100.0 0.1   // order 1: amount=100, tax=10
        item 2 102 2  50.0 0.2   // order 2: amount=100, tax=20
    ]
    let result = calculatetotalAmountAndTaxByOrderId items
    Assert.Equal(2, result.Length)
    let r1 = result |> List.find (fun o -> o.order_id = 1)
    let r2 = result |> List.find (fun o -> o.order_id = 2)
    Assert.Equal(100.0, r1.total_amount)
    Assert.Equal(10.0,  r1.total_tax)
    Assert.Equal(100.0, r2.total_amount)
    Assert.Equal(20.0,  r2.total_tax)

[<Fact>]
let ``calculatetotalAmountAndTaxByOrderId handles single item`` () =
    let items = [ item 5 201 3 25.0 0.1 ]   // amount=75, tax=7.5
    let result = calculatetotalAmountAndTaxByOrderId items
    Assert.Equal(1, result.Length)
    let r = result.[0]
    Assert.Equal(5, r.order_id)
    Assert.Equal(75.0, r.total_amount)
    Assert.Equal(7.5,  r.total_tax, 10)

[<Fact>]
let ``calculatetotalAmountAndTaxByOrderId returns 0 for zero quantity items`` () =
    let items = [ item 1 101 0 50.0 0.2 ]   // 0 * 50.0 = 0
    let result = calculatetotalAmountAndTaxByOrderId items
    let r = result |> List.find (fun o -> o.order_id = 1)
    Assert.Equal(0.0, r.total_amount)
    Assert.Equal(0.0, r.total_tax)

[<Fact>]
let ``calculatetotalAmountAndTaxByOrderId returns 0 tax for zero-tax items`` () =
    let items = [
        item 1 101 2 10.0 0.0
        item 1 102 3 20.0 0.0
    ]
    let result = calculatetotalAmountAndTaxByOrderId items
    let r = result |> List.find (fun o -> o.order_id = 1)
    Assert.Equal(80.0, r.total_amount)   // 2*10 + 3*20 = 80
    Assert.Equal(0.0,  r.total_tax)

[<Fact>]
let ``calculatetotalAmountAndTaxByOrderId output order_id matches the grouped order`` () =
    let items = [
        item 42 301 1 10.0 0.1
        item 42 302 1 20.0 0.1
    ]
    let result = calculatetotalAmountAndTaxByOrderId items
    Assert.Equal(1, result.Length)
    Assert.Equal(42, result.[0].order_id)
