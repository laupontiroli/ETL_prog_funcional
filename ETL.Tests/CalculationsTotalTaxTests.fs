module CalculationsTotalTaxTests

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

// --- calculateTotalTax ---
// formula: qty * tax * price

[<Fact>]
let ``calculateTotalTax returns correct sum for matching order_id`` () =
    let items = [
        item 1 101 2 10.0 0.1   // 2 * 0.1 * 10.0 = 2.0
        item 1 102 3 5.0  0.05  // 3 * 0.05 * 5.0  = 0.75
        item 2 103 1 50.0 0.2   // different order, must be ignored
    ]
    let result = calculateTotalTax 1 (items |> Seq.ofList)
    Assert.Equal(2.75, result)

[<Fact>]
let ``calculateTotalTax returns 0 for empty sequence`` () =
    let result = calculateTotalTax 1 (Seq.empty)
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalTax returns 0 when order_id has no matches`` () =
    let items = [
        item 1 101 2 10.0 0.1
        item 2 102 1 20.0 0.2
    ]
    let result = calculateTotalTax 999 (items |> Seq.ofList)
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalTax returns 0 when tax is 0`` () =
    let items = [ item 1 101 5 100.0 0.0 ]   // 5 * 0.0 * 100.0 = 0.0
    let result = calculateTotalTax 1 (items |> Seq.ofList)
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalTax returns 0 when quantity is 0`` () =
    let items = [ item 1 101 0 100.0 0.2 ]   // 0 * 0.2 * 100.0 = 0.0
    let result = calculateTotalTax 1 (items |> Seq.ofList)
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalTax returns correct value for single item`` () =
    let items = [ item 3 201 4 25.0 0.1 ]   // 4 * 0.1 * 25.0 = 10.0
    let result = calculateTotalTax 3 (items |> Seq.ofList)
    Assert.Equal(10.0, result)

[<Fact>]
let ``calculateTotalTax sums all matching items and ignores non-matching`` () =
    let items = [
        item 5 301 1 100.0 0.2   // 1 * 0.2 * 100.0 = 20.0
        item 5 302 2  50.0 0.1   // 2 * 0.1 *  50.0 = 10.0
        item 6 303 3  10.0 0.3   // different order
    ]
    let result = calculateTotalTax 5 (items |> Seq.ofList)
    Assert.Equal(30.0, result)
