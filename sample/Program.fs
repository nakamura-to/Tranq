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

type Person = { [<Id>]Id: int; Name: string; Email: Email.t; Age: Age.t; [<Version>]Version: Version.t }

let setup = txSupports { 
  do! 
    Db.run "
      if exists (select * from dbo.sysobjects where id = object_id(N'Person')) drop table Person;
      create table Person (Id int primary key, Name varchar(20), Email varchar(50), Age int, Version int);
      " []
  let! _ = 
    Db.insert<Person> {
      Id = 1
      Name = "hoge"
      Email = Email.create "hoge@example.com"
      Age = Age.create <| Some 10
      Version = Version.create -1 } 
  let! _ = 
    Db.insert<Person> {
      Id = 2
      Name = "foo"
      Email = Email.create "foo@example.com"
      Age = Age.create None
      Version = Version.create -1 } 
  let! _ = 
    Db.insert<Person> {
      Id = 3
      Name = "bar"
      Email = Email.create "bar@example.com"
      Age = Age.create <| Some 20
      Version = Version.create -1 } 
  return ()}

let queryAll () = txSupports {
  let! persons = Db.query<Person> "select * from Person" []
  return persons }

let queryByName (name: string) = txSupports {
  return! Db.query<Person> "select * from Person where Name = @name" ["@name" <-- name]}

let update person = txSupports {
  let! _ = Db.update<Person> person
  return ()}

let txMain = txRequired {
  do! setup
  let! persons = queryAll()
  let persons = 
    persons 
    |> Seq.map (fun p -> { p with Age = Age.incr p.Age })
    // ideally `Seq.toList` is unneccessary, but current implementation requires it cause of a bug 
    |> Seq.toList 
  for p in persons do
    do! update p
  let! persons = queryAll()
  return Seq.toList persons }

let dataConvRepo =
  let repo = DataConvRepo()
  repo.Add(Email.conv)
  repo.Add(Age.conv)
  repo.Add(Version.conv)
  repo
  
let config = {
  Dialect = MsSqlDialect(dataConvRepo)
  ConnectionProvider = fun () -> 
    new SqlConnection("Data Source=.\SQLEXPRESS;Initial Catalog=tempdb;Integrated Security=True;") :> DbConnection
  Logger = fun stmt -> printfn "LOG: %s" stmt.FormattedText }

let run txBlock =
  match runTxBlock config txBlock with
  | Success r -> printfn "success: %A\n" r
  | Failure exn -> printfn "failure: %A\n" exn

[<EntryPoint>]
let main argv = 
  run txMain
  System.Console.ReadKey() |> fun _ -> 0
