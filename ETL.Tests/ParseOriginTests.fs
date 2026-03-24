module ParseOriginTests

open Xunit
open ETL.Types
open ETL.Transformations

// --- parseOrigin ---

[<Fact>]
let ``parseOrigin returns O for "O"`` () =
    Assert.Equal(O, parseOrigin "O")

[<Fact>]
let ``parseOrigin returns P for "P"`` () =
    Assert.Equal(P, parseOrigin "P")

[<Fact>]
let ``parseOrigin throws on unknown value`` () =
    Assert.Throws<exn>(fun () -> parseOrigin "X" |> ignore)

[<Fact>]
let ``parseOrigin throws on empty string`` () =
    Assert.Throws<exn>(fun () -> parseOrigin "" |> ignore)

[<Fact>]
let ``parseOrigin is case-sensitive and throws on lowercase`` () =
    Assert.Throws<exn>(fun () -> parseOrigin "o" |> ignore)

[<Fact>]
let ``parseOrigin throws on whitespace string`` () =
    Assert.Throws<exn>(fun () -> parseOrigin " " |> ignore)
