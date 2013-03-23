#r @"..\..\packages\FSPowerPack.Core.Community.3.0.0.0\Lib\Net40\FSharp.PowerPack.dll"
#r @"lib\Tranq.dll"
#load @"util.fsx"

open Tranq

type Person = { [<Id>]Id: int; Email: string; Age: int option; [<Version>]Version: int }

let workflow = txRequired {
  do! Db.run "
      if exists (select * from dbo.sysobjects where id = object_id(N'Person')) drop table Person;
      create table Person (Id int primary key, Email varchar(50), Age int, Version int);
      insert Person (Id, Email, Age, Version) values (1, 'hoge@example.com', 10, 0);
      insert Person (Id, Email, Age, Version) values (2, 'foo_example.com', null, 0);
      insert Person (Id, Email, Age, Version) values (3, 'bar@example.com', 30, 0);
      insert Person (Id, Email, Age, Version) values (4, 'fuga@example.com', 40, 0);
      insert Person (Id, Email, Age, Version) values (5, 'piyo@example.com', 50, 0);
      " []
  return! Db.query<Person> "
    select * from Person where Age > @age order by Id
    " [ "@age" <-- 30 ]  }

Util.eval workflow |> function
| Success ret -> printfn "success: %A\n" ret
| Failure exn -> printfn "failure: %A\n" exn