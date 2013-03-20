# Quick Start

## Create a F# Application Project

In Visual Stuido, create a F# application project.

## Install Tranq with Nuget

Install the `Tranq` library into your project.

```
PM> Install-Package Tranq
```

## Make a Database Table

In this example, we use `tempdb`.

Create a table and insert data.

```sql
use tempdb;
create table Person (Id int primary key, Name varchar(50), Age int);
insert Person (Id, Name, Age) values (1, 'hoge', 10);
```

## Write Code

### Open Tranq namespace

```fsharp
open Tranq
```

### Define a Record Type

```fsharp
type Person = { [<Id>]Id: int; Name: string; Age: int }
```
Specify `Tranq.IdAttribute` to the identifier field.

### Define Transaction Workflow

```fsharp
let workflow = txRequired {
  let! person = Db.find<Person> [1]
  return! Db.update { person with Age = person.Age + 1 }}
```

Find a row and update it.

### Define a Configuration

```fsharp
let config =
  let provide() =
    let conStr = "Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True;" 
    new System.Data.SqlClient.SqlConnection(conStr) :> System.Data.Common.DbConnection
  let log = function
    | SqlIssuing(_, stmt) -> printfn "LOG: %s" stmt.FormattedText
    |_ -> ()
  { Dialect = MsSqlDialect(); ConnectionProvider = provide; Listener = log }
```

### Evaluate a workflow

Evaluate the workflow and get a result.

```fsharp
Tx.eval config workflow |> function
| Success ret -> printfn "success: %A\n" ret
| Failure exn -> printfn "failure: %A\n" exn
```

### Entire Source Code

```fsharp
open Tranq

type Person = { [<Id>]Id: int; Name: string; Age: int }

let workflow = txRequired {
  let! person = Db.find<Person> [1]
  return! Db.update { person with Age = person.Age + 1 }}

let config =
  let provide() =
    let conStr = "Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True;" 
    new System.Data.SqlClient.SqlConnection(conStr) :> System.Data.Common.DbConnection
  let log = function
    | SqlIssuing(_, stmt) -> printfn "LOG: %s" stmt.FormattedText
    |_ -> ()
  { Dialect = MsSqlDialect(); ConnectionProvider = provide; Listener = log }

Tx.eval config workflow |> function
| Success ret -> printfn "success: %A\n" ret
| Failure exn -> printfn "failure: %A\n" exn
```

## Run

Run the application with Ctrl + F5.

The console shows as follows: 

```
LOG: select Id, Name, Age from Person where Id = 1
LOG: update Person set Name = N'hoge', Age = 11 where Id = 1
success: {Id = 1; Name = "hoge"; Age = 11;}
```
