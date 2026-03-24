module ParseFilterStatusTests

open Xunit
open ETL.Types
open ETL.Transformations

// --- parseFilterStatus ---

[<Fact>]
let ``parseFilterStatus returns Pending for "P"`` () =
    Assert.Equal(Pending, parseFilterStatus "P")

[<Fact>]
let ``parseFilterStatus returns Complete for "C"`` () =
    Assert.Equal(Complete, parseFilterStatus "C")

[<Fact>]
let ``parseFilterStatus returns Cancelled for "X"`` () =
    Assert.Equal(Cancelled, parseFilterStatus "X")

[<Fact>]
let ``parseFilterStatus throws when value is "-s" sentinel`` () =
    Assert.Throws<exn>(fun () -> parseFilterStatus "-s" |> ignore)

[<Fact>]
let ``parseFilterStatus throws on unknown value`` () =
    Assert.Throws<exn>(fun () -> parseFilterStatus "Z" |> ignore)

[<Fact>]
let ``parseFilterStatus throws on empty string`` () =
    Assert.Throws<exn>(fun () -> parseFilterStatus "" |> ignore)

[<Fact>]
let ``parseFilterStatus is case-sensitive and throws on lowercase`` () =
    Assert.Throws<exn>(fun () -> parseFilterStatus "p" |> ignore)

[<Fact>]
let ``parseFilterStatus throws on whitespace string`` () =
    Assert.Throws<exn>(fun () -> parseFilterStatus " " |> ignore)
