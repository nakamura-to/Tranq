# Quick Start

## Create a F# Application Project

In Visual Stuido, create a new F# application project.

## Install Tranq with Nuget

Install the `Tranq` library into the project.

```
PM> Install-Package Tranq
```

## Make a Database Table

In this example, we use SQL Server 2012 Express.

Create a table in tempdb and insert data.

```sql
use tempdb;
if exists (select * from dbo.sysobjects where id = object_id(N'Person')) drop table Person;
create table Person (Id int primary key, Name varchar(50), Age int);
insert Person (Id, Name, Age) values (1, 'hoge', 10);
```

## Write Code

Open a Program.fs file and paste following code.

```fsharp
open Tranq

// This is mapped to a Database Person Table.
// Tranq.IdAttribute is required to the identifier field.
type Person = { [<Id>]Id: int; Name: string; Age: int }

// transaction workflow
let workflow = txRequired {
  // find by identifier
  let! person = Db.find<Person> [1]
  // update
  return! Db.update { person with Age = person.Age + 1 }}

// Tranq configuration
let config =
  let provide() =
    let connectionString = "Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True;" 
    new System.Data.SqlClient.SqlConnection(connectionString) :> System.Data.Common.DbConnection
  let log = function
    | SqlIssuing(_, stmt) -> printfn "LOG: %s" stmt.FormattedText
    |_ -> ()
  { Dialect = MsSqlDialect(); ConnectionProvider = provide; Listener = log }

// evaluate the transaction workflow and get a result
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
