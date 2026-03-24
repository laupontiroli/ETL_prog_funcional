module AverageTaxAndAmountByMonthTests

open Xunit
open ETL.Types
open ETL.Calculations

let private item year month price tax : OrderItemWithOrderInfo =
    { order_id = 1
      product_id = 1
      quantity = 1
      price = price
      tax = tax
      status = Pending
      origin = O
      date = System.DateTime(year, month, 1) }

// --- calculateAverageTaxAndAmountByMonthAndYear ---

[<Fact>]
let ``calculateAverageTaxAndAmountByMonthAndYear returns empty list for empty input`` () =
    let result = calculateAverageTaxAndAmountByMonthAndYear []
    Assert.Empty(result)

[<Fact>]
let ``calculateAverageTaxAndAmountByMonthAndYear returns one group for single item`` () =
    let items = [ item 2024 3 50.0 0.15 ]
    let result = calculateAverageTaxAndAmountByMonthAndYear items
    Assert.Equal(1, result.Length)
    let r = result.[0]
    Assert.Equal(3, r.month)
    Assert.Equal(2024, r.year)
    Assert.Equal(0.15, r.average_tax)
    Assert.Equal(50.0, r.average_amount)

[<Fact>]
let ``calculateAverageTaxAndAmountByMonthAndYear groups items by month`` () =
    let items = [
        item 2024 1 10.0 0.1
        item 2024 1 20.0 0.2
        item 2024 2 30.0 0.3
    ]
    let result = calculateAverageTaxAndAmountByMonthAndYear items
    Assert.Equal(2, result.Length)

[<Fact>]
let ``calculateAverageTaxAndAmountByMonthAndYear calculates correct averages per group`` () =
    let items = [
        item 2024 1 10.0 0.1
        item 2024 1 30.0 0.3
    ]
    let result = calculateAverageTaxAndAmountByMonthAndYear items
    let jan = result |> List.find (fun r -> r.month = 1 && r.year = 2024)
    Assert.Equal(0.2, jan.average_tax, 10)
    Assert.Equal(20.0, jan.average_amount)

[<Fact>]
let ``calculateAverageTaxAndAmountByMonthAndYear separates same month across different years`` () =
    let items = [
        item 2023 6 100.0 0.1
        item 2024 6 200.0 0.2
    ]
    let result = calculateAverageTaxAndAmountByMonthAndYear items
    Assert.Equal(2, result.Length)
    let r2023 = result |> List.find (fun r -> r.year = 2023)
    let r2024 = result |> List.find (fun r -> r.year = 2024)
    Assert.Equal(100.0, r2023.average_amount)
    Assert.Equal(0.1,   r2023.average_tax)
    Assert.Equal(200.0, r2024.average_amount)
    Assert.Equal(0.2,   r2024.average_tax)

[<Fact>]
let ``calculateAverageTaxAndAmountByMonthAndYear produces one group per distinct month-year pair`` () =
    let items = [
        item 2024  1 10.0 0.1
        item 2024  2 20.0 0.2
        item 2024  3 30.0 0.3
        item 2025  1 40.0 0.4
    ]
    let result = calculateAverageTaxAndAmountByMonthAndYear items
    Assert.Equal(4, result.Length)

[<Fact>]
let ``calculateAverageTaxAndAmountByMonthAndYear averages three items in same month`` () =
    let items = [
        item 2024 5 10.0 0.1
        item 2024 5 20.0 0.2
        item 2024 5 30.0 0.3
    ]
    let result = calculateAverageTaxAndAmountByMonthAndYear items
    Assert.Equal(1, result.Length)
    let r = result.[0]
    Assert.Equal(5, r.month)
    Assert.Equal(2024, r.year)
    Assert.Equal(0.2, r.average_tax, 10)
    Assert.Equal(20.0, r.average_amount)

[<Fact>]
let ``calculateAverageTaxAndAmountByMonthAndYear handles items with zero price and tax`` () =
    let items = [
        item 2024 8 0.0 0.0
        item 2024 8 0.0 0.0
    ]
    let result = calculateAverageTaxAndAmountByMonthAndYear items
    Assert.Equal(1, result.Length)
    let r = result.[0]
    Assert.Equal(0.0, r.average_tax)
    Assert.Equal(0.0, r.average_amount)
