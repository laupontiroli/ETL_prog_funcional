module Tests

open Xunit
open ETL.Types
open ETL.Calculations

// --- helpers ---

let private item orderId productId qty price tax =
    { order_id = orderId; product_id = productId; quantity = qty; price = price; tax = tax }

// --- calculateTotalAmount ---

[<Fact>]
let ``calculateTotalAmount retorna a soma correta`` () =
    let items = [
        item 1 101 2 10.0 0.1   // 2 * 10.0 = 20.0
        item 1 102 3 5.0  0.05  // 3 *  5.0 = 15.0
        item 2 103 1 50.0 0.2   // different order, must be ignored
    ]
    let result = calculateTotalAmount 1 items
    Assert.Equal(35.0, result)

[<Fact>]
let ``calculateTotalAmount  retorna 0 quando não houver itens`` () =
    let items = [
        item 2 101 2 10.0 0.1
        item 3 102 1 20.0 0.2
    ]
    let result = calculateTotalAmount 99 items
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalAmount retorna 0 quando a sequência estiver vazia`` () =
    let result = calculateTotalAmount 1 []
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalAmount calcula o total correto para um único item`` () =
    let items = [ item 5 200 4 7.5 0.1 ]  // 4 * 7.5 = 30.0
    let result = calculateTotalAmount 5 items
    Assert.Equal(30.0, result)

[<Fact>]
let ``calculateTotalAmount ignora todos os itens quando o order_id não existe`` () =
    let items = [ item 1 101 10 100.0 0.0; item 2 102 5 50.0 0.0 ]
    let result = calculateTotalAmount 999 items
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalAmount lida corretamente com quantidade de 0`` () =
    let items = [ item 1 101 0 99.99 0.1 ]  // 0 * 99.99 = 0.0
    let result = calculateTotalAmount 1 items
    Assert.Equal(0.0, result)

[<Fact>]
let ``calculateTotalAmount lida corretamente com múltiplos itens todos pertencentes ao mesmo order_id`` () =
    let items = [
        item 7 301 1 100.0 0.1   // 100.0
        item 7 302 2 50.0  0.05  // 100.0
        item 7 303 5 20.0  0.2   // 100.0
    ]
    let result = calculateTotalAmount 7 items
    Assert.Equal(300.0, result)

// --- parseStatus ---

[<Fact>]
let ``parseStatus returns Pending for "Pending"`` () =
    Assert.Equal(Pending, ETL.Transformations.parseStatus "Pending")

[<Fact>]
let ``parseStatus returns Complete for "Complete"`` () =
    Assert.Equal(Complete, ETL.Transformations.parseStatus "Complete")

[<Fact>]
let ``parseStatus returns Cancelled for "Cancelled"`` () =
    Assert.Equal(Cancelled, ETL.Transformations.parseStatus "Cancelled")

[<Fact>]
let ``parseStatus throws on unknown value`` () =
    Assert.Throws<exn>(fun () -> ETL.Transformations.parseStatus "Unknown" |> ignore)

// --- parseOrigin ---

[<Fact>]
let ``parseOrigin returns O for "O"`` () =
    Assert.Equal(O, ETL.Transformations.parseOrigin "O")

[<Fact>]
let ``parseOrigin returns P for "P"`` () =
    Assert.Equal(P, ETL.Transformations.parseOrigin "P")

[<Fact>]
let ``parseOrigin throws on unknown value`` () =
    Assert.Throws<exn>(fun () -> ETL.Transformations.parseOrigin "X" |> ignore)
