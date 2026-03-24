module CalculationsTotalAmountTests

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

// --- calculateTotalAmount ---
// formula: qty * price

[<Fact>]
let ``calculateTotalAmount retorna a soma correta`` () =
    let items: OrderItemWithOrderInfo list = [
        item 1 101 2 10.0 0.1   // 2 * 10.0 = 20.0
        item 1 102 3 5.0  0.05  // 3 *  5.0 = 15.0
        item 2 103 1 50.0 0.2   // different order, must be ignored
    ]
    let result = calculateTotalAmount 1 (items |> Seq.ofList)
    Assert.Equal(35.0, result)

[<Fact>]
let ``calculateTotalAmount retorna 0 quando não houver itens`` () =
    let items = [
        item 2 101 2 10.0 0.1
        item 3 102 1 20.0 0.2
    ]
    let result = calculateTotalAmount 99 (items |> Seq.ofList)
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalAmount retorna 0 quando a sequência estiver vazia`` () =
    let result = calculateTotalAmount 1 []
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalAmount calcula o total correto para um único item`` () =
    let items = [ item 5 200 4 7.5 0.1 ]  // 4 * 7.5 = 30.0
    let result = calculateTotalAmount 5 (items |> Seq.ofList)
    Assert.Equal(30.0, result)

[<Fact>]
let ``calculateTotalAmount ignora todos os itens quando o order_id não existe`` () =
    let items = [ item 1 101 10 100.0 0.0; item 2 102 5 50.0 0.0 ]
    let result = calculateTotalAmount 999 (items |> Seq.ofList)
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalAmount lida corretamente com quantidade de 0`` () =
    let items = [ item 1 101 0 99.99 0.1 ]  // 0 * 99.99 = 0.0
    let result = calculateTotalAmount 1 (items |> Seq.ofList)
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalAmount lida corretamente com múltiplos itens todos pertencentes ao mesmo order_id`` () =
    let items = [
        item 7 301 1 100.0 0.1   // 100.0
        item 7 302 2 50.0  0.05  // 100.0
        item 7 303 5 20.0  0.2   // 100.0
    ]
    let result = calculateTotalAmount 7 (items |> Seq.ofList)
    Assert.Equal(300.0, result)

[<Fact>]
let ``calculateTotalAmount ignora o imposto no cálculo`` () =
    let items = [ item 1 101 2 10.0 0.99 ]  // tax must not affect result: 2 * 10.0 = 20.0
    let result = calculateTotalAmount 1 (items |> Seq.ofList)
    Assert.Equal(20.0, result)
