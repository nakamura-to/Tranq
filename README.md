# F# DB Access Library

Tranq is DB Access Library by F#, for F#.

Tranq implements transaction computation expressions and rich type mappings.

Following RDBMSs are supported

- SQL Server

## Install

To install Tranq, run the following command in the Package Manager Console

```
PM> Install-Package Tranq
```

### Global Transactions

If you need global transactions, install Tranq.GlobalTx too. This is optional.

```
PM> Install-Package Tranq.GlobalTx
```

To use global transactions, open `Tranq.GlobalTx` namespace after opening `Tranq` namespace.

```fsharp
open Tranq
open Tranq.GlobalTx
```

## Transaction Computation Expressions

```fsharp
type Person = { [<Id>]Id: int; Name: string; Age: int }

let workflow = txRequired {
  let! person = Db.find<Person> [1]
  Db.update { person with Age = person.Age + 1 }}

Tx.eval config workflow |> function
| Success ret -> printfn "success: %A\n" ret
| Failure exn -> printfn "failure: %A\n" exn
```

The `txRequired` means that the expression is always executed in a transaction.

Other than `txRequired`, following keywords are available

- txRequiresNew - the expression is always executed in a NEW transaction
- txSupports - the expression adopt the exact transactional state of the caller
- txNotSupported - the expression never be executed in a transaction

## Rich Type Mappings

You can map a Database column value to a discriminated union using `IDataConv` interface.

```fsharp
type Name = Name of string
type Age = Age of int

let config = 
  let registry = DataConvRegistry()
  registry.Add({ new IDataConv<Name, string> with
    member this.Compose(v) = Name v
    member this.Decompose(Name(v)) = v })
  registry.Add({ new IDataConv<Age, int> with
    member this.Compose(v) = Age v
    member this.Decompose(Age(v)) = v })
  { Dialect = MsSqlDialect(registry)
    ConnectionProvider = fun () -> 
      new SqlConnection("Data Source=.\SQLEXPRESS;Integrated Security=True;" ) :> DbConnection
    Listener = fun event -> printfn "%A" event }

type Person = { [<Id>]Id: int; Name: Name; Age: Age }

let workflow = txRequired {
  let! person = Db.find<Person> [1]
  printfn "%A" person.Name }

Tx.eval config workflow |> function
| Success ret -> printfn "success: %A\n" ret
| Failure exn -> printfn "failure: %A\n" exn
```

## Query, Command and Stored Procedure

### Find by Id and Map a result to a Record

```fsharp
type Person = { [<Id>]Id: int; Name: string; Age: int }

let workflow = txRequired {
  let! person = Db.find<Person> [1]
  printfn "%s" person.Name }
```

### Map results to a Record List

```fsharp
type Person = { [<Id>]Id: int; Name: string; Age: int }

let workflow = txRequired {
  let! persons = Db.query<Person> "select * from Person where Name = @name" ["@name" <-- "Jhon"]
  persons |> List.iter (fun p -> printfn "%s" p.Name) }
```

### Map results to a Tuple List

```fsharp
let workflow = txRequired {
  let! persons = Db.query<string * int> "select Name, Age from Person where Name = @name" ["@name" <-- "Jhon"]
  persons |> List.iter (fun (name, age) -> printfn "%s %d" name age) }
```

### Execute a Command

```fsharp
type Person = { [<Id>]Id: int; Name: string; Age: int }

let workflow = txRequired {
  let! person = Db.insert { Id = 99; Name = "King"; Age = 31}
  let! person = Db.update { person with Age = person.Age + 1 }
  do! Db.delete person }
```

### Execute a Stored Procedure

```fsharp
type Person = { [<Id>]Id: int; Name: string; Age: int }
type GetPersons = { 
  Age : int
  [<ProcedureParam(Direction = Direction.Result)>]
  Persons : Person list }

let workflow = txRequired {
  let! result = Db.call { Age = 20; Persons = [] } 
  result.Persons |> List.iter (fun p -> printfn "%s" p.Name) }
```

## SQL Template

### Parameters

SQL parameters are indicated with `@` prefix.

```fsharp
Db.query<Person> "select * from Person where Name = @name" ["@name" <-- "Jhon"]
```

You can also specify SQL parameters as set of comment and test data.
Following query is equivalent to above one.

```fsharp
Db.query<Person> "select * from Person where Name = /*name*/'test'" ["name" <-- "Jhon"]
```

### List Parameter Support

Following two queries are equivalent.

```fsharp
Db.query<Person> "select * from Person where Age in @ages" ["@ages" <-- [10; 20; 30]]
```

```fsharp
Db.query<Person> "select * from Person where Age in /* ages */(1,2,3)" ["@ages" <-- [10; 20; 30]]
```

SQL is built as follows

```sql
select * from Person where Age in (@p0, @p1, @p2)
```

### IF Expression

The `if` expression is only available in a clause, such as SELECT clase and WHERE clause and so on.
Be carefule that it is unable to straddle more than two clauses.

```fsharp
type Person = { [<Id>]Id: int; Name: string; Age: int }

let workflow (name : string option) = txRequired {
  let! persons = 
    Db.query<Person> "
      select * from Person where
      /*%if isSome age*/
        Name = @name
      /*%end*/
      " ["@name" <-- name]
  persons |> List.iter (fun p -> printfn "%s" p.Name) }
```

If the name parameter is `Some`, SQL is built as follows

```sql
select * from Person where Name = @p0
```

If the name parameter is `None`, SQL is built as follows

```sql
select * from Person
```

## License

Apache License, Version 2.0

http://www.apache.org/licenses/LICENSE-2.0.html