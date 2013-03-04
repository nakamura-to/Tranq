open System.Data.Common
open System.Data.SqlClient
open Tranq

module Email =
  type t = Email of string
  let create = Email
  let conv = { new IDataConv<t, string> with
    member this.Compose(value) = create value
    member this.Decompose(Email(value)) = value }

module Age =
  type t = Age of int option
  let create = Age
  let conv = { new IDataConv<t, int option> with
    member this.Compose(value) = create value
    member this.Decompose(Age(value)) = value }
  let incr (Age(age)) =
    age |> Option.map ((+) 1) |> create

module Version =
  type t = Version of int
  let create = Version
  let conv = { new IDataConv<t, int> with
    member this.Compose(value) = create value
    member this.Decompose(Version(value)) = value }

module Person =
  [<Table(Name = "Person")>]
  type t = { [<Id>]Id: int; Name: string; Email: Email.t; Age: Age.t; [<Version>]Version: Version.t }
  let incrAge person = { person with Age = Age.incr person.Age }

module Dao =
  let setup = txSupports { 
    do! 
      Db.run "
        if exists (select * from dbo.sysobjects where id = object_id(N'Person')) drop table Person;
        create table Person (Id int primary key, Name varchar(20), Email varchar(50), Age int, Version int);
        " []
    let! _ = 
      Db.insert<Person.t> {
        Id = 1
        Name = "hoge"
        Email = Email.create "hoge@example.com"
        Age = Age.create <| Some 10
        Version = Version.create -1 } 
    let! _ = 
      Db.insert<Person.t> {
        Id = 2
        Name = "foo"
        Email = Email.create "foo@example.com"
        Age = Age.create None
        Version = Version.create -1 } 
    let! _ = 
      Db.insert<Person.t> {
        Id = 3
        Name = "bar"
        Email = Email.create "bar@example.com"
        Age = Age.create <| Some 20
        Version = Version.create -1 } 
    return ()}

  let queryPersonAll () = txSupports {
    return! Db.query<Person.t> "select * from Person" [] }

  let queryPersonByName (name: string) = txSupports {
    return! Db.query<Person.t> "select * from Person where Name = @name" ["@name" <-- name]}

  let updatePerson person = txSupports {
    let! _ = Db.update<Person.t> person
    return ()}

let workflow = txRequired {
  do! Dao.setup
  let! persons = Dao.queryPersonAll()
  for p in persons |> Seq.map Person.incrAge do
    do! Dao.updatePerson p
  let! p1 = Dao.queryPersonByName "hoge"
  let! p2 = Dao.queryPersonByName "foo"
  let! p3 = Dao.queryPersonByName "bar"
  return [p1; p2; p3] }

let config = 
  let dataConvRepo =
    let repo = DataConvRepo()
    repo.Add(Email.conv)
    repo.Add(Age.conv)
    repo.Add(Version.conv)
    repo
  { Dialect = MsSqlDialect(dataConvRepo)
    ConnectionProvider = fun () -> 
      new SqlConnection("Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True;") :> DbConnection
    Logger = fun stmt -> printfn "LOG: %s" stmt.FormattedText }

let run txBlock =
  match runTxBlock config txBlock with
  | Success ret -> printfn "success: %A\n" ret
  | Failure exn -> printfn "failure: %A\n" exn

[<EntryPoint>]
let main argv = 
  run workflow
  System.Console.ReadKey() |> fun _ -> 0
