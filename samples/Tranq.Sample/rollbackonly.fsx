#r @"..\..\packages\FSPowerPack.Core.Community.3.0.0.0\Lib\Net40\FSharp.PowerPack.dll"
#r @"lib\Tranq.dll"

open Tranq

// entity
type Person = { [<Id>]Id: int; Email: string; Age: int option; [<Version>]Version: int }

// configuration
let config =
  let provide() =
    let conStr = "Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True;" 
    new System.Data.SqlClient.SqlConnection(conStr) :> System.Data.Common.DbConnection
  let log = function
    | SqlIssuing(_, stmt) -> printfn "LOG: %s" stmt.FormattedText
    |_ -> ()
  { Dialect = MsSqlDialect(); ConnectionProvider =provide; Listener = log }

// transaction workflow
let workflow = txRequired {
  do! Db.run "
      if exists (select * from dbo.sysobjects where id = object_id(N'Person')) drop table Person;
      create table Person (Id int primary key, Email varchar(50), Age int, Version int);
      " []
  do! Db.insert<Person> {Id = 1; Email = "hoge@example.com"; Age = Some 20; Version = 0} |> Tx.ignore
  let! person = Db.find<Person> [1]
  do! Tx.rollbackOnly
  let! person = Db.update<Person> { person with Email = "hoge@sample.com" }
  do! Db.delete<Person> person }

// eval transaction workflow
Tx.eval config workflow |> function
| Success ret -> printfn "success: %A\n" ret
| Failure exn -> printfn "failure: %A\n" exn
