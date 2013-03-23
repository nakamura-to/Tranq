#r @"..\..\packages\FSPowerPack.Core.Community.3.0.0.0\Lib\Net40\FSharp.PowerPack.dll"
#r @"lib\Tranq.dll"
#load @"util.fsx"

open Tranq

type Person = { [<Id>]Id: int; Email: string; Age: int option; [<Version>]Version: int }

let workflow = txRequired {
  do! Db.run "
      if exists (select * from dbo.sysobjects where id = object_id(N'Person')) drop table Person;
      create table Person (Id int primary key, Email varchar(50), Age int, Version int);
      " []
  do! Db.insert<Person> {Id = 1; Email = "hoge@example.com"; Age = Some 20; Version = 0} |> Tx.ignore
  let! person = Db.find<Person> [1]
  let! person = Db.update<Person> { person with Email = "hoge@sample.com" }
  do! Db.delete<Person> person }

Util.eval workflow |> function
| Success ret -> printfn "success: %A\n" ret
| Failure exn -> printfn "failure: %A\n" exn
