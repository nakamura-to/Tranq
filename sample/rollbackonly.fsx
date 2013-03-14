#r @"bin\Debug\FSharp.PowerPack.dll"
#r @"bin\Debug\Tranq.dll"

open Tranq

// entity
type Person = { [<Id>]Id: int; Email: string; Age: int option; [<Version>]Version: int }

// configuration
let config =
  let provide() =
    let conStr = "Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True;" 
    new System.Data.SqlClient.SqlConnection(conStr) :> System.Data.Common.DbConnection
  let log = function
    | Sql(_, stmt) -> printfn "LOG: %s" stmt.FormattedText
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
  // rollback only
  do! Tx.rollbackOnly
  let! person = Db.update<Person> { person with Email = "hoge@sample.com" }
  do! Db.delete<Person> person }

// eval transaction workflow
Tx.eval config workflow |> function
| Success ret -> printfn "success: %A\n" ret
| Failure exn -> printfn "failure: %A\n" exn

(* OUTPUT
LOG: if exists (select * from dbo.sysobjects where id = object_id(N'Person')) drop table Person;
      create table Person (Id int primary key, Email varchar(50), Age int, Version int);
LOG: insert into Person ( Id, Email, Age, Version ) values ( 1, N'hoge@example.com', 20, 1 )
LOG: select Id, Email, Age, Version from Person where Id = 1
LOG: update Person set Email = N'hoge@sample.com', Age = 20, Version = Version + 1 where Id = 1 and Version = 1
LOG: delete from Person where Id = 1 and Version = 2
success: <null>
*)
