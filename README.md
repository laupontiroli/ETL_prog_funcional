# ETL em Programação Funcional (F#)

Projeto ETL (_Extract, Transform, Load_) implementado em F# como demonstração do paradigma funcional aplicado ao processamento de dados. O pipeline lê dois arquivos CSV de pedidos, realiza transformações e agregações, e grava os resultados em novos arquivos CSV ou em um banco de dados SQLite.

---

## Arquitetura

O projeto é uma _solution_ .NET 9 com três subprojetos:

```
ETL_prog_funcional.sln
├── ETL/                  # Biblioteca principal (funções puras e impuras)
│   ├── Types.fs          # Tipos de domínio (Order, OrderItem, Output, …)
│   ├── HelperFunctions.fs# Funções puras: parsing, join, cálculos
│   └── ExternalFunctions.fs # Funções impuras: leitura CSV/HTTP e escrita CSV/SQLite
├── Main/
│   └── Program.fs        # Ponto de entrada; orquestra o pipeline e lê flags da CLI
├── ETL.Tests/            # Testes unitários com xUnit (funções puras)
│   └── *.fs              # 13 arquivos de teste
└── DataIn/
    ├── order.csv         # Arquivo de entrada: pedidos
    └── order_item.csv    # Arquivo de entrada: itens dos pedidos
```

### Separação puro/impuro

| Arquivo | Natureza | Responsabilidade |
|---|---|---|
| `ETL/Types.fs` | puro | Definição de tipos e unions discriminadas |
| `ETL/HelperFunctions.fs` | puro | Parsing, join, `map`/`fold`/`filter`, agregações |
| `ETL/ExternalFunctions.fs` | impuro | Leitura de CSV (local ou HTTP), escrita em CSV e SQLite |
| `Main/Program.fs` | impuro | Entrada do programa, leitura de args, composição do pipeline |

---

## Como Rodar

### Pré-requisitos

- [.NET 9 SDK](https://dotnet.microsoft.com/download)

### Executar o pipeline

```bash
# Fonte local (DataIn/) → saída em DataOut/PatternOutput.csv + DataOut/AverageOutput.csv
dotnet run --project Main

# Fonte externa (HTTP, GitHub raw) → mesma saída em CSV
dotnet run --project Main -- -e

# Saída em banco de dados SQLite (DataOut/output.db) em vez de CSV
dotnet run --project Main -- -d

# Filtrar por status: P (Pending), C (Complete), X (Cancelled)
dotnet run --project Main -- -s C

# Filtrar por origem: O (Online), P (Physical)
dotnet run --project Main -- -o O

# Combinações são permitidas
dotnet run --project Main -- -e -s C -o O -d
```

### Rodar os testes

```bash
dotnet test
```

---

## Saídas Geradas

| Arquivo | Condição | Conteúdo |
|---|---|---|
| `DataOut/PatternOutput.csv` | sem `-d` | `order_id`, `total_amount`, `total_tax` por pedido |
| `DataOut/AverageOutput.csv` | sem `-d` | `average_tax`, `average_amount`, `month`, `year` |
| `DataOut/output.db` | com `-d` | Tabela `output` no SQLite com `order_id`, `total_amount`, `total_tax` |

Exemplo de `PatternOutput.csv`:

```
order_id,total_amount,total_tax
13,1976.99,220.24
2,2982.1,323.18
```

Exemplo de `AverageOutput.csv`:

```
average_tax,average_amount,month,year
0.13,126.38,1,2025
0.1,98.9,8,2024
```

---

## Como o Pipeline Foi Construído

### 1. Extração

`ExternalFunctions.Input.loadOrders` e `loadOrderItems` usam `FSharp.Data.CsvFile.Load` para carregar os arquivos (path local ou URL HTTP). Cada linha é convertida em um Record F# via _helper functions_ (`rowToOrder`, `rowToOrderItem`), que extraem cada campo individualmente — cumprindo o requisito de uso obrigatório de helper functions.

### 2. Transformação

Após a extração, as duas listas são combinadas via **inner join** implementado em F# (`joinOrderWithItems`): cada `OrderItem` é enriquecido com dados do `Order` correspondente usando um `Map<int, Order>` para lookup O(1). O resultado é uma lista de `OrderItemWithOrderInfo`.

Os cálculos são realizados exclusivamente com funções de ordem superior:
- **`Seq.filter`** — filtragem por `order_id` ao calcular totais, e filtragem por `status`/`origin` na CLI
- **`Seq.map`** — transformação de items em valores numéricos (`quantity × price`)
- **`Seq.fold`** — redução (soma) dos valores mapeados para obter o total

Os filtros opcionais de `-s` (status) e `-o` (origin) são aplicados sobre a lista de `OrderItemWithOrderInfo` antes dos cálculos, garantindo que o pipeline inteiro trabalhe apenas com os dados relevantes.

### 3. Carga (Load)

Duas saídas são possíveis, controladas pela flag `-d`:
- **CSV**: `WriteCSVOutput` usa reflexão F# (`FSharpType.GetRecordFields`) para serializar qualquer tipo de Record genericamente — sem necessidade de código específico por tipo.
- **SQLite**: `WriteDBOutput` usa `Microsoft.Data.Sqlite` para criar a tabela `output` (se não existir) e inserir/atualizar os registros via `INSERT OR REPLACE`.

---

## Testes

Os testes cobrem todas as funções puras do módulo `ETL` usando xUnit:

| Arquivo de Teste | Funções cobertas |
|---|---|
| `ParseStatusTests.fs` | `parseStatus` |
| `ParseOriginTests.fs` | `parseOrigin` |
| `ParseFilterStatusTests.fs` | `parseFilterStatus` |
| `ParseFilterOriginTests.fs` | `parseFilterOrigin` |
| `RowToOrderTests.fs` | `rowToOrder` |
| `RowToOrderItemTests.fs` | `rowToOrderItem` |
| `Round2Tests.fs` | `round2` |
| `CalculationsTotalAmountTests.fs` | `calculateTotalAmount` |
| `CalculationsTotalTaxTests.fs` | `calculateTotalTax` |
| `JoinOrderWithItemsTests.fs` | `joinOrderWithItems` |
| `CalculateTotalAmountAndTaxByOrderIdTests.fs` | `calculatetotalAmountAndTaxByOrderId` |
| `CalculateAverageTests.fs` | `calculateAverageTax`, `calculateAverageAmount` |
| `AverageTaxAndAmountByMonthTests.fs` | `calculateAverageTaxAndAmountByMonthAndYear` |

---

## Uso de IA Generativa

Este projeto foi desenvolvido com auxílio de IA Generativa (Cursor / Claude). A utilização foi voltada para auxilio de correção de escrita do readME, auxilio na escrita dos testes das funções pela quantidade e correção de erros durante o desenvolvimento.

---

## Checklist de Requisitos

### Requisitos Base (obrigatórios)

| # | Requisito | Status |
|---|---|---|
| 1 | Projeto implementado em F# | ✅ |
| 2 | Uso de `map`, `fold` e `filter` nos cálculos | ✅ |
| 3 | Funções de leitura e escrita de CSV | ✅ |
| 4 | Separação entre funções puras e impuras | ✅ |
| 5 | Entrada carregada em lista de Records | ✅ |
| 6 | Helper functions para carregar campos em Records | ✅ |
| 7 | Relatório do projeto (este README) | ✅ |

### Requisitos Extras

| # | Requisito | Status |
|---|---|---|
| 1 | Leitura de dados de fonte HTTP estática (`-e`) | ✅ |
| 2 | Saída em banco de dados relacional SQLite (`-d`) | ✅ |
| 3 | Inner join das tabelas em F# | ✅ |
| 4 | Projeto organizado como solution .NET com múltiplos projetos | ✅ |
| 5 | Documentação de todas as funções via docstring (`///`) | ✅ |
| 6 | Saída adicional com média de receita e impostos por mês/ano | ✅ |
| 7 | Arquivos de testes completos para as funções puras | ✅ |
