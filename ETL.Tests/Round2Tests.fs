module Round2Tests

open Xunit
open ETL.Calculations

// --- round2 ---

[<Fact>]
let ``round2 returns value unchanged when already two decimal places`` () =
    Assert.Equal(1.23, round2 1.23)

[<Fact>]
let ``round2 rounds down when third decimal is less than 5`` () =
    Assert.Equal(1.23, round2 1.234)

[<Fact>]
let ``round2 rounds up when third decimal is greater than 5`` () =
    Assert.Equal(1.24, round2 1.236)

[<Fact>]
let ``round2 returns 0.0 for 0.0`` () =
    Assert.Equal(0.0, round2 0.0)

[<Fact>]
let ``round2 handles whole numbers`` () =
    Assert.Equal(10.0, round2 10.0)

[<Fact>]
let ``round2 handles negative values`` () =
    Assert.Equal(-1.23, round2 -1.234)
