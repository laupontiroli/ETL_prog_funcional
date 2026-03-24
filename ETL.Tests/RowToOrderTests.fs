module RowToOrderTests

open Xunit
open FSharp.Data
open ETL.Types
open ETL.Transformations

let private makeOrderRow id clientId (date: string) status origin =
    let csv = CsvFile.Parse($"id,client_id,order_date,status,origin\n{id},{clientId},{date},{status},{origin}")
    csv.Rows |> Seq.head

// --- rowToOrder ---

[<Fact>]
let ``rowToOrder parses a valid Pending/O row correctly`` () =
    let row = makeOrderRow 1 10 "2024-03-15" "Pending" "O"
    let result = rowToOrder row
    Assert.Equal(1, result.id)
    Assert.Equal(10, result.client_id)
    Assert.Equal(System.DateTime(2024, 3, 15), result.order_date)
    Assert.Equal(Pending, result.status)
    Assert.Equal(O, result.origin)

[<Fact>]
let ``rowToOrder parses Complete status correctly`` () =
    let row = makeOrderRow 2 20 "2023-06-01" "Complete" "P"
    let result = rowToOrder row
    Assert.Equal(Complete, result.status)
    Assert.Equal(P, result.origin)

[<Fact>]
let ``rowToOrder parses Cancelled status correctly`` () =
    let row = makeOrderRow 3 30 "2022-12-31" "Cancelled" "O"
    let result = rowToOrder row
    Assert.Equal(Cancelled, result.status)

[<Fact>]
let ``rowToOrder throws on unknown status`` () =
    let row = makeOrderRow 4 40 "2024-01-01" "Unknown" "O"
    Assert.ThrowsAny<System.Exception>(fun () -> rowToOrder row |> ignore) |> ignore

[<Fact>]
let ``rowToOrder throws on unknown origin`` () =
    let row = makeOrderRow 5 50 "2024-01-01" "Pending" "Z"
    Assert.ThrowsAny<System.Exception>(fun () -> rowToOrder row |> ignore) |> ignore

[<Fact>]
let ``rowToOrder throws on invalid date format`` () =
    let row = makeOrderRow 6 60 "not-a-date" "Pending" "O"
    Assert.ThrowsAny<System.Exception>(fun () -> rowToOrder row |> ignore) |> ignore
