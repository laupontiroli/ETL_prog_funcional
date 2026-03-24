module ParseFilterOriginTests

open Xunit
open ETL.Types
open ETL.Transformations

// --- parseFilterOrigin ---

[<Fact>]
let ``parseFilterOrigin returns O for "O"`` () =
    Assert.Equal(O, parseFilterOrigin "O")

[<Fact>]
let ``parseFilterOrigin returns P for "P"`` () =
    Assert.Equal(P, parseFilterOrigin "P")

[<Fact>]
let ``parseFilterOrigin throws when value is "-o" sentinel`` () =
    Assert.Throws<exn>(fun () -> parseFilterOrigin "-o" |> ignore)

[<Fact>]
let ``parseFilterOrigin throws on unknown value`` () =
    Assert.Throws<exn>(fun () -> parseFilterOrigin "X" |> ignore)

[<Fact>]
let ``parseFilterOrigin throws on empty string`` () =
    Assert.Throws<exn>(fun () -> parseFilterOrigin "" |> ignore)

[<Fact>]
let ``parseFilterOrigin is case-sensitive and throws on lowercase`` () =
    Assert.Throws<exn>(fun () -> parseFilterOrigin "o" |> ignore)

[<Fact>]
let ``parseFilterOrigin throws on whitespace string`` () =
    Assert.Throws<exn>(fun () -> parseFilterOrigin " " |> ignore)
