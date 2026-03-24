namespace ETL.ExternalFunctions

open FSharp.Data
open ETL.Types

/// Funções de leitura de arquivos CSV (local ou via HTTP).
module Input =

    /// Carrega uma lista de <c>Order</c> a partir de um arquivo ou URL CSV.
    let loadOrders(path: string) : Order list =
        CsvFile.Load(path).Rows
        |> Seq.map ETL.Transformations.rowToOrder
        |> Seq.toList

    /// Carrega uma lista de <c>OrderItem</c> a partir de um arquivo ou URL CSV.
    let loadOrderItems(path: string) : OrderItem list =
        CsvFile.Load(path).Rows
        |> Seq.map ETL.Transformations.rowToOrderItem
        |> Seq.toList


/// Funções de escrita dos resultados em CSV ou banco de dados SQLite.
module Output =

    /// Serializa qualquer sequência de registros F# em um arquivo CSV,
    /// usando reflexão para inferir o cabeçalho e os valores das colunas.
    let WriteCSVOutput (output: 'a seq) (path: string) : unit =
        let t = typeof<'a>
        let fields = Microsoft.FSharp.Reflection.FSharpType.GetRecordFields(t)
        let header = fields |> Array.map (fun f -> f.Name) |> String.concat ","
        let rows =
            output
            |> Seq.map (fun record ->
                fields
                |> Array.map (fun f -> f.GetValue(record) |> string)
                |> String.concat ",")
        System.IO.Directory.CreateDirectory(System.IO.Path.GetDirectoryName(path)) |> ignore
        System.IO.File.WriteAllLines(path, Seq.append [header] rows)

    /// Persiste uma sequência de <c>Output</c> na tabela <c>output</c> do banco SQLite
    /// em <c>DataOut/output.db</c>, criando a tabela se necessário.
    let WriteDBOutput (output: Output seq) : unit =
        let dbPath = System.IO.Path.Combine(__SOURCE_DIRECTORY__, "..", "DataOut", "output.db")
        System.IO.Directory.CreateDirectory(System.IO.Path.GetDirectoryName(dbPath)) |> ignore
        use conn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbPath}")
        conn.Open()
        use createCmd = conn.CreateCommand()
        createCmd.CommandText <-
            """CREATE TABLE IF NOT EXISTS output (
                order_id     INTEGER PRIMARY KEY,
                total_amount REAL NOT NULL,
                total_tax    REAL NOT NULL
            )"""
        createCmd.ExecuteNonQuery() |> ignore
        for row in output do
            use insertCmd = conn.CreateCommand()
            insertCmd.CommandText <-
                "INSERT OR REPLACE INTO output (order_id, total_amount, total_tax) VALUES (@order_id, @total_amount, @total_tax)"
            insertCmd.Parameters.AddWithValue("@order_id", row.order_id) |> ignore
            insertCmd.Parameters.AddWithValue("@total_amount", row.total_amount) |> ignore
            insertCmd.Parameters.AddWithValue("@total_tax", row.total_tax) |> ignore
            insertCmd.ExecuteNonQuery() |> ignore