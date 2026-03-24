module ParseStatusTests

open Xunit
open ETL.Types
open ETL.Transformations

// --- parseStatus ---

[<Fact>]
let ``parseStatus returns Pending for "Pending"`` () =
    Assert.Equal(Pending, parseStatus "Pending")

[<Fact>]
let ``parseStatus returns Complete for "Complete"`` () =
    Assert.Equal(Complete, parseStatus "Complete")

[<Fact>]
let ``parseStatus returns Cancelled for "Cancelled"`` () =
    Assert.Equal(Cancelled, parseStatus "Cancelled")

[<Fact>]
let ``parseStatus throws on unknown value`` () =
    Assert.Throws<exn>(fun () -> parseStatus "Unknown" |> ignore)

[<Fact>]
let ``parseStatus throws on empty string`` () =
    Assert.Throws<exn>(fun () -> parseStatus "" |> ignore)

[<Fact>]
let ``parseStatus is case-sensitive and throws on lowercase`` () =
    Assert.Throws<exn>(fun () -> parseStatus "pending" |> ignore)

[<Fact>]
let ``parseStatus throws on whitespace string`` () =
    Assert.Throws<exn>(fun () -> parseStatus " " |> ignore)
